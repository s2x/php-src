/* Plain HTTP gateway for a pool (--with-fpm-http, needs libevent).
 *
 * For every TCP pool the master forks a few gateway processes that serve
 * plain HTTP on the FastCGI port + 1 with libevent's evhttp, sharing one
 * listening socket. A gateway behaves like a web server in front of the pool:
 * it talks FastCGI to the pool over a small set of persistent connections,
 * sends the request as FastCGI records and turns the FastCGI response back
 * into HTTP. Neither the FastCGI code nor the PHP workers know that HTTP
 * exists. Keep-alive, chunked request bodies, HEAD and request parsing are
 * evhttp's job. Without libevent the gateway is compiled out and FPM behaves
 * as before.
 *
 * Persistent connections: a kept FastCGI connection pins one PHP worker, so
 * the gateways together never hold more than pm.max_children of them and
 * requests beyond that wait in the gateway's own queue instead of the kernel
 * backlog. A worker waiting for the next request on a kept connection counts
 * as active, so dynamic spawns spare workers for everyone else as it should.
 * ondemand never reaps such a worker though, and with any pm a pinned worker
 * is unavailable to other FastCGI clients (nginx, the status page), so an
 * idle connection is dropped after FPM_HTTP_IDLE_MS (env override, 0 keeps
 * them forever).
 */

#include "fpm_config.h"

#include "fpm.h"
#include "fpm_http.h"

#ifdef HAVE_FPM_HTTP

#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>

#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>
#include <event2/util.h>

#include "php.h"
#include "fastcgi.h"
#include "zend_smart_str.h"

#include "fpm_conf.h"
#include "fpm_worker_pool.h"
#include "fpm_sockets.h"
#include "fpm_cleanup.h"
#include "fpm_signals.h"
#include "fpm_env.h"
#include "zlog.h"

#define FPM_HTTP_GATEWAYS_DEFAULT 2			/* overridable with FPM_HTTP_GATEWAYS until there is a directive */
#define FPM_HTTP_IDLE_MS         500		/* release a pinned worker after this much idle time */
#define FPM_HTTP_MAX_BODY        (32 * 1024 * 1024)
#define FPM_HTTP_MAX_CGI_HEADERS (64 * 1024)
#define FCGI_MAX_RECORD_LEN      0xffff
#define FPM_HTTP_BAD_GATEWAY     502 /* libevent has no constant for it */

typedef struct _fpm_http_conn fpm_http_conn;
typedef struct _fpm_http_upstream fpm_http_upstream;

/* one gateway family per pool */
struct fpm_http_gateway_s {
	struct fpm_http_gateway_s *next;
	char *pool;
	char *listen_address;			/* where the pool takes FastCGI */
	char *docroot;
	int listen_fd;
	int backlog;
	int reuseport;					/* every gateway binds its own SO_REUSEPORT socket (FPM_HTTP_REUSEPORT=1) */
	unsigned nproc;
	pid_t *pids;

	/* gateway process only */
	unsigned max_upstreams;			/* this process' share of the pool's workers */
	struct event_base *base;
	struct evhttp *http;
	struct sockaddr_storage upstream_addr;
	socklen_t upstream_len;
	TAILQ_HEAD(, _fpm_http_upstream) upstreams;
	unsigned nupstreams;
	TAILQ_HEAD(, _fpm_http_conn) waiting;	/* requests without a free connection yet */
};

static struct fpm_http_gateway_s *gateways = NULL;

/* one HTTP request being proxied */
struct _fpm_http_conn {
	struct fpm_http_gateway_s *gw;
	struct evhttp_request *req;
	struct evhttp_connection *evcon;
	fpm_http_upstream *upstream;		/* while in flight */
	int queued;
	TAILQ_ENTRY(_fpm_http_conn) link;

	smart_str params;					/* FCGI_PARAMS payload being assembled */
	smart_str out;						/* records ready to go upstream */

	smart_str cgi_headers;				/* CGI header block until it is complete */
	int headers_sent;
};

/* One persistent FastCGI connection to the pool, serving one request at a time.
 * Plain socket rather than a bufferevent: the read event is registered once and
 * never touched, and writes go straight out, which keeps epoll_ctl and the
 * FIONREAD ioctl that evbuffer_read does out of the hot path. */
struct _fpm_http_upstream {
	struct fpm_http_gateway_s *gw;
	int fd;
	struct event *ev_read;
	struct event *ev_write;				/* only pending while a write did not fit or we are connecting */
	int connecting;
	smart_str pending;					/* not yet written to the pool */
	size_t pending_off;
	int busy;							/* a request is in flight, even if its client is gone */
	fpm_http_conn *current;				/* NULL when idle or when the client went away */
	TAILQ_ENTRY(_fpm_http_upstream) link;

	/* FastCGI record stream from the pool */
	unsigned char rec_hdr[8];
	int rec_hdr_len, rec_type, rec_len, rec_pad;
};

static void fpm_http_pump(struct fpm_http_gateway_s *gw);

/* EAGAIN and EWOULDBLOCK are the same value on most systems, hence the macro dance */
static inline int fpm_http_would_block(int err)
{
	if (err == EAGAIN) {
		return 1;
	}
#if EWOULDBLOCK != EAGAIN
	if (err == EWOULDBLOCK) {
		return 1;
	}
#endif
	return 0;
}
static int fpm_http_listen(const char *pool, const char *listen_address, int backlog, int reuseport);

static struct timeval idle_timeout = {FPM_HTTP_IDLE_MS / 1000, (FPM_HTTP_IDLE_MS % 1000) * 1000};
static int idle_ms = FPM_HTTP_IDLE_MS;		/* FPM_HTTP_IDLE_MS env overrides it; 0 = never close */

/* ---------------------------------------------------------------- FastCGI encoding */

static void fpm_http_fcgi_record(smart_str *out, int type, const char *data, size_t len)
{
	unsigned char hdr[8] = {FCGI_VERSION_1, (unsigned char)type, 0, 1, (unsigned char)(len >> 8), (unsigned char)len, (unsigned char)((8 - len % 8) % 8), 0};
	static const char zeros[8] = {0};

	smart_str_appendl(out, (char*)hdr, sizeof(hdr));
	smart_str_appendl(out, data, len);
	smart_str_appendl(out, zeros, hdr[6]);
}

static void fpm_http_fcgi_len(smart_str *out, size_t len)
{
	if (len < 0x80) {
		smart_str_appendc(out, (char)len);
	} else {
		unsigned char b[4] = {(unsigned char)((len >> 24) | 0x80), (unsigned char)(len >> 16), (unsigned char)(len >> 8), (unsigned char)len};
		smart_str_appendl(out, (char*)b, 4);
	}
}

/* name/value pairs never straddle records, the receiver decodes each record on its own */
static void fpm_http_param(fpm_http_conn *c, const char *name, const char *value)
{
	size_t name_len = strlen(name), value_len = strlen(value);
	size_t pair_len = (name_len < 0x80 ? 1 : 4) + (value_len < 0x80 ? 1 : 4) + name_len + value_len;

	if (c->params.s && ZSTR_LEN(c->params.s) + pair_len > FCGI_MAX_RECORD_LEN) {
		fpm_http_fcgi_record(&c->out, FCGI_PARAMS, ZSTR_VAL(c->params.s), ZSTR_LEN(c->params.s));
		smart_str_free(&c->params);
	}
	fpm_http_fcgi_len(&c->params, name_len);
	fpm_http_fcgi_len(&c->params, value_len);
	smart_str_appendl(&c->params, name, name_len);
	smart_str_appendl(&c->params, value, value_len);
}

/* ---------------------------------------------------------------- request -> FastCGI */

static const char *fpm_http_method_name(enum evhttp_cmd_type type)
{
	switch (type) {
		case EVHTTP_REQ_GET: return "GET";
		case EVHTTP_REQ_POST: return "POST";
		case EVHTTP_REQ_HEAD: return "HEAD";
		case EVHTTP_REQ_PUT: return "PUT";
		case EVHTTP_REQ_DELETE: return "DELETE";
		case EVHTTP_REQ_OPTIONS: return "OPTIONS";
		case EVHTTP_REQ_TRACE: return "TRACE";
		case EVHTTP_REQ_CONNECT: return "CONNECT";
		case EVHTTP_REQ_PATCH: return "PATCH";
	}
	return NULL;
}

/* Builds BEGIN_REQUEST, PARAMS and STDIN in c->out. Returns an HTTP error code or 0. */
static int fpm_http_build_request(fpm_http_conn *c)
{
	static const char begin_request[8] = {0, FCGI_RESPONDER, FCGI_KEEP_CONN, 0, 0, 0, 0, 0};
	struct evhttp_request *req = c->req;
	const struct evhttp_uri *uri = evhttp_request_get_evhttp_uri(req);
	const char *method = fpm_http_method_name(evhttp_request_get_command(req));
	const char *path = uri ? evhttp_uri_get_path(uri) : NULL;
	const char *query = uri ? evhttp_uri_get_query(uri) : NULL;
	const char *host = evhttp_request_get_host(req);
	struct evkeyval *header;
	struct evbuffer *body = evhttp_request_get_input_buffer(req);
	char *decoded, *peer_addr = NULL, buf[64];
	ev_uint16_t peer_port = 0;
	smart_str filename = {0};
	const char *path_info;
	size_t decoded_len, body_len = evbuffer_get_length(body);

	if (!method || !path || !*path) {
		return HTTP_BADREQUEST;
	}

	/* SCRIPT_NAME is the decoded path, SCRIPT_FILENAME puts it under the document root */
	decoded = evhttp_uridecode(path, 0, &decoded_len);
	if (!decoded) {
		return HTTP_BADREQUEST;
	}
	if (decoded_len != strlen(decoded) || /* embedded NUL */
	    strstr(decoded, "/../") ||
	    (decoded_len >= 3 && memcmp(decoded + decoded_len - 3, "/..", 3) == 0)) {
		free(decoded);
		return HTTP_BADREQUEST;
	}
	/* Split like nginx' fastcgi_split_path_info ^(.+?\.php)(/.*)$: everything up to and
	 * including the first ".php" is the script, the rest is PATH_INFO. Doing it here rather
	 * than letting FPM stat its way to the script keeps one syscall out of every request. */
	path_info = strstr(decoded, ".php/");
	if (path_info) {
		path_info += sizeof(".php") - 1;
	}
	smart_str_appends(&filename, c->gw->docroot);
	smart_str_appendl(&filename, decoded, path_info ? (size_t)(path_info - decoded) : decoded_len);
	if (decoded[decoded_len - 1] == '/') {
		smart_str_appendl(&filename, "index.php", sizeof("index.php") - 1);
	}
	smart_str_0(&filename);

	snprintf(buf, sizeof(buf), "HTTP/%d.%d", req->major, req->minor);
	fpm_http_param(c, "REQUEST_METHOD", method);
	fpm_http_param(c, "SERVER_PROTOCOL", buf);
	fpm_http_param(c, "GATEWAY_INTERFACE", "CGI/1.1");
	fpm_http_param(c, "SERVER_SOFTWARE", "PHP-FPM/" PHP_VERSION);
	fpm_http_param(c, "REQUEST_URI", evhttp_request_get_uri(req));
	fpm_http_param(c, "QUERY_STRING", query ? query : "");
	fpm_http_param(c, "DOCUMENT_ROOT", c->gw->docroot);
	fpm_http_param(c, "SCRIPT_NAME", ZSTR_VAL(filename.s) + strlen(c->gw->docroot));
	fpm_http_param(c, "SCRIPT_FILENAME", ZSTR_VAL(filename.s));
	if (path_info) {
		fpm_http_param(c, "PATH_INFO", path_info);
	}
	smart_str_free(&filename);
	free(decoded);

	if (host) {
		char *server_name = strdup(host), *colon = strrchr(server_name, ':');

		if (colon && !strchr(colon, ']')) {
			*colon = '\0';
		}
		fpm_http_param(c, "SERVER_NAME", server_name);
		free(server_name);
	}
	evhttp_connection_get_peer(c->evcon, &peer_addr, &peer_port);
	if (peer_addr) {
		fpm_http_param(c, "REMOTE_ADDR", peer_addr);
		snprintf(buf, sizeof(buf), "%u", (unsigned)peer_port);
		fpm_http_param(c, "REMOTE_PORT", buf);
	}
	/* the body is complete (and de-chunked) at this point, so the length is ours to state */
	snprintf(buf, sizeof(buf), "%zu", body_len);
	fpm_http_param(c, "CONTENT_LENGTH", buf);

	/* "Content-Type: x" -> CONTENT_TYPE, anything else -> HTTP_<UPPER_WITH_UNDERSCORES> */
	TAILQ_FOREACH(header, evhttp_request_get_input_headers(req), next) {
		smart_str name = {0};
		const char *k = header->key;

		if (strcasecmp(k, "Content-Length") == 0) {
			continue;
		}
		if (strcasecmp(k, "Content-Type") != 0) {
			smart_str_appendl(&name, "HTTP_", sizeof("HTTP_") - 1);
		}
		for (; *k; k++) {
			smart_str_appendc(&name, *k == '-' ? '_' : toupper((unsigned char)*k));
		}
		smart_str_0(&name);
		fpm_http_param(c, ZSTR_VAL(name.s), header->value);
		smart_str_free(&name);
	}

	/* BEGIN_REQUEST, PARAMS (possibly several records), empty PARAMS, STDIN, empty STDIN */
	fpm_http_fcgi_record(&c->out, FCGI_BEGIN_REQUEST, begin_request, sizeof(begin_request));
	if (c->params.s) {
		fpm_http_fcgi_record(&c->out, FCGI_PARAMS, ZSTR_VAL(c->params.s), ZSTR_LEN(c->params.s));
	}
	fpm_http_fcgi_record(&c->out, FCGI_PARAMS, "", 0);
	if (body_len) {
		const char *data = (const char*)evbuffer_pullup(body, -1);
		size_t off;

		for (off = 0; off < body_len; off += FCGI_MAX_RECORD_LEN) {
			fpm_http_fcgi_record(&c->out, FCGI_STDIN, data + off, MIN(body_len - off, FCGI_MAX_RECORD_LEN));
		}
	}
	fpm_http_fcgi_record(&c->out, FCGI_STDIN, "", 0);
	return 0;
}

/* ---------------------------------------------------------------- FastCGI -> response */

static void fpm_http_conn_free(fpm_http_conn *c)
{
	if (c->evcon) {
		evhttp_connection_set_closecb(c->evcon, NULL, NULL);
	}
	if (c->queued) {
		TAILQ_REMOVE(&c->gw->waiting, c, link);
	}
	smart_str_free(&c->params);
	smart_str_free(&c->out);
	smart_str_free(&c->cgi_headers);
	free(c);
}

/* The CGI header block is complete: "Status:" becomes the status line, the rest is copied. */
static void fpm_http_start_reply(fpm_http_conn *c, size_t head_len, size_t body_off)
{
	struct evkeyvalq *out = evhttp_request_get_output_headers(c->req);
	const char *line = c->cgi_headers.s ? ZSTR_VAL(c->cgi_headers.s) : "", *end = line + head_len;
	char *reason = NULL;
	int code = HTTP_OK;

	while (line < end) {
		const char *nl = memchr(line, '\n', end - line), *next = nl ? nl + 1 : end, *colon;
		size_t len = (nl ? nl : end) - line;

		if (len && line[len - 1] == '\r') {
			len--;
		}
		if ((colon = memchr(line, ':', len))) {
			size_t klen = colon - line;
			const char *v = colon + 1;
			size_t vlen = len - klen - 1;
			char *key, *value;

			while (vlen && (*v == ' ' || *v == '\t')) {
				v++;
				vlen--;
			}
			key = strndup(line, klen);
			value = strndup(v, vlen);
			if (strcasecmp(key, "Status") == 0) {
				code = atoi(value);
				free(reason);
				reason = strdup(strchr(value, ' ') ? strchr(value, ' ') + 1 : "");
			} else {
				evhttp_add_header(out, key, value);
			}
			free(key);
			free(value);
		}
		line = next;
	}

	evhttp_send_reply_start(c->req, code, reason && *reason ? reason : NULL);
	free(reason);
	c->headers_sent = 1;

	if (c->cgi_headers.s && body_off < ZSTR_LEN(c->cgi_headers.s)) {
		struct evbuffer *chunk = evbuffer_new();

		evbuffer_add(chunk, ZSTR_VAL(c->cgi_headers.s) + body_off, ZSTR_LEN(c->cgi_headers.s) - body_off);
		evhttp_send_reply_chunk(c->req, chunk);
		evbuffer_free(chunk);
	}
	smart_str_free(&c->cgi_headers);
}

static void fpm_http_stdout(fpm_http_conn *c, const char *data, size_t len)
{
	size_t scan_from, i;
	const char *h;

	if (c->headers_sent) {
		struct evbuffer *chunk = evbuffer_new();

		evbuffer_add(chunk, data, len);
		evhttp_send_reply_chunk(c->req, chunk);
		evbuffer_free(chunk);
		return;
	}

	scan_from = c->cgi_headers.s && ZSTR_LEN(c->cgi_headers.s) > 3 ? ZSTR_LEN(c->cgi_headers.s) - 3 : 0;
	smart_str_appendl(&c->cgi_headers, data, len);
	smart_str_0(&c->cgi_headers);
	h = ZSTR_VAL(c->cgi_headers.s);

	for (i = scan_from; i + 1 < ZSTR_LEN(c->cgi_headers.s); i++) {
		if (h[i] == '\n' && h[i + 1] == '\n') {
			fpm_http_start_reply(c, i + 1, i + 2);
			return;
		}
		if (i + 3 < ZSTR_LEN(c->cgi_headers.s) && memcmp(h + i, "\r\n\r\n", 4) == 0) {
			fpm_http_start_reply(c, i + 2, i + 4);
			return;
		}
	}
	if (ZSTR_LEN(c->cgi_headers.s) > FPM_HTTP_MAX_CGI_HEADERS) {
		fpm_http_start_reply(c, 0, 0); /* no header block in sight, ship it as a body */
	}
}

/* The pool is done with the request (END_REQUEST seen or the connection failed). */
static void fpm_http_finish(fpm_http_conn *c, int upstream_ok)
{
	if (c->headers_sent) {
		evhttp_send_reply_end(c->req);
	} else if (c->cgi_headers.s) {
		fpm_http_start_reply(c, 0, 0); /* partial header block, ship what we have */
		evhttp_send_reply_end(c->req);
	} else {
		if (!upstream_ok) {
			zlog(ZLOG_WARNING, "[pool %s] http: no answer from '%s'", c->gw->pool, c->gw->listen_address);
		}
		evhttp_send_error(c->req, FPM_HTTP_BAD_GATEWAY, "Bad Gateway");
	}
	fpm_http_conn_free(c);
}

/* ---------------------------------------------------------------- upstream connections */

static void fpm_http_upstream_drop(fpm_http_upstream *up)
{
	TAILQ_REMOVE(&up->gw->upstreams, up, link);
	up->gw->nupstreams--;
	if (up->ev_read) {
		event_free(up->ev_read);
	}
	if (up->ev_write) {
		event_free(up->ev_write);
	}
	close(up->fd);
	smart_str_free(&up->pending);
	free(up);
}

/* the pool went away mid-request or while idle */
static void fpm_http_upstream_fail(fpm_http_upstream *up, int clean_eof)
{
	struct fpm_http_gateway_s *gw = up->gw;

	if (!clean_eof) {
		zlog(ZLOG_WARNING, "[pool %s] http: upstream '%s': %s", gw->pool, gw->listen_address, strerror(errno));
	}
	/* EOF is normal after pm.max_requests or a worker restart; a request in flight is lost though */
	if (up->current) {
		fpm_http_finish(up->current, clean_eof);
		up->current = NULL;
	}
	fpm_http_upstream_drop(up);
	fpm_http_pump(gw);
}

/* one request finished on this connection, it is free for the next */
static void fpm_http_request_done(fpm_http_upstream *up)
{
	if (up->current) {
		fpm_http_finish(up->current, 1);
		up->current = NULL;
	}
	up->busy = 0;
	memset(up->rec_hdr, 0, sizeof(up->rec_hdr));
	up->rec_hdr_len = up->rec_type = up->rec_len = up->rec_pad = 0;
	if (idle_ms > 0) {
		event_add(up->ev_read, &idle_timeout);
	}
	fpm_http_pump(up->gw);
}

/* Feeds bytes from the pool into the record parser. */
static void fpm_http_upstream_data(fpm_http_upstream *up, const char *buf, size_t len)
{
	while (len > 0) {
		size_t take;

		if (up->rec_len == 0 && up->rec_pad == 0) {
			take = MIN(sizeof(up->rec_hdr) - up->rec_hdr_len, len);
			memcpy(up->rec_hdr + up->rec_hdr_len, buf, take);
			up->rec_hdr_len += take;
			buf += take;
			len -= take;
			if (up->rec_hdr_len < (int)sizeof(up->rec_hdr)) {
				break;
			}
			up->rec_hdr_len = 0;
			up->rec_type = up->rec_hdr[1];
			up->rec_len = (up->rec_hdr[4] << 8) | up->rec_hdr[5];
			up->rec_pad = up->rec_hdr[6];
			if (up->rec_len == 0 && up->rec_pad == 0 && up->rec_type == FCGI_END_REQUEST) {
				fpm_http_request_done(up);
			}
			continue;
		}
		if (up->rec_len > 0) {
			take = MIN((size_t)up->rec_len, len);
			if (up->rec_type == FCGI_STDOUT && up->current) {
				fpm_http_stdout(up->current, buf, take);
			} else if (up->rec_type == FCGI_STDERR) {
				zlog(ZLOG_NOTICE, "[pool %s] http: %.*s", up->gw->pool, (int)take, buf);
			}
			up->rec_len -= take;
		} else {
			take = MIN((size_t)up->rec_pad, len);
			up->rec_pad -= take;
		}
		buf += take;
		len -= take;
		if (up->rec_len == 0 && up->rec_pad == 0 && up->rec_type == FCGI_END_REQUEST) {
			fpm_http_request_done(up);
		}
	}
}

static void fpm_http_upstream_readcb(evutil_socket_t fd, short what, void *arg)
{
	fpm_http_upstream *up = arg;
	char buf[16 * 1024];
	ssize_t n;

	if (what & EV_TIMEOUT) {
		/* idle for a while: give the worker back to the pool */
		if (!up->busy) {
			fpm_http_upstream_drop(up);
		}
		return;
	}
	do {
		n = read(fd, buf, sizeof(buf));
	} while (n < 0 && errno == EINTR);

	if (n > 0) {
		fpm_http_upstream_data(up, buf, n);
	} else if (n == 0 || !fpm_http_would_block(errno)) {
		fpm_http_upstream_fail(up, n == 0);
	}
}

/* Writes whatever is pending; registers the write event only when the socket is full. */
static void fpm_http_upstream_flush(fpm_http_upstream *up)
{
	while (up->pending.s && up->pending_off < ZSTR_LEN(up->pending.s)) {
		ssize_t n = write(up->fd, ZSTR_VAL(up->pending.s) + up->pending_off, ZSTR_LEN(up->pending.s) - up->pending_off);

		if (n > 0) {
			up->pending_off += n;
		} else if (n < 0 && errno == EINTR) {
			continue;
		} else if (n < 0 && fpm_http_would_block(errno)) {
			event_add(up->ev_write, NULL);
			return;
		} else {
			fpm_http_upstream_fail(up, 0);
			return;
		}
	}
	smart_str_free(&up->pending);
	up->pending_off = 0;
}

static void fpm_http_upstream_writecb(evutil_socket_t fd, short what, void *arg)
{
	fpm_http_upstream *up = arg;

	if (up->connecting) {
		int err = 0;
		socklen_t len = sizeof(err);

		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
			errno = err;
			fpm_http_upstream_fail(up, 0);
			return;
		}
		up->connecting = 0;
		event_add(up->ev_read, NULL);
	}
	fpm_http_upstream_flush(up);
}

static void fpm_http_upstream_write(fpm_http_upstream *up, const char *data, size_t len)
{
	smart_str_appendl(&up->pending, data, len);
	if (!up->connecting) {
		fpm_http_upstream_flush(up);
	}
}

static fpm_http_upstream *fpm_http_upstream_new(struct fpm_http_gateway_s *gw)
{
	fpm_http_upstream *up = calloc(1, sizeof(*up));

	up->gw = gw;
	up->fd = socket(gw->upstream_addr.ss_family, SOCK_STREAM, 0);
	if (up->fd < 0) {
		free(up);
		return NULL;
	}
	evutil_make_socket_nonblocking(up->fd);
	if (gw->upstream_addr.ss_family != AF_UNIX) {
		int on = 1;

		setsockopt(up->fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
	}
	up->ev_read = event_new(gw->base, up->fd, EV_READ | EV_PERSIST, fpm_http_upstream_readcb, up);
	up->ev_write = event_new(gw->base, up->fd, EV_WRITE, fpm_http_upstream_writecb, up);

	if (connect(up->fd, (struct sockaddr*)&gw->upstream_addr, gw->upstream_len) != 0) {
		if (errno != EINPROGRESS) {
			event_free(up->ev_read);
			event_free(up->ev_write);
			close(up->fd);
			free(up);
			return NULL;
		}
		up->connecting = 1;
		event_add(up->ev_write, NULL);
	} else {
		event_add(up->ev_read, NULL);
	}
	TAILQ_INSERT_TAIL(&gw->upstreams, up, link);
	gw->nupstreams++;
	return up;
}

/* Hands waiting requests to free connections, opening new ones up to this process' share. */
static void fpm_http_pump(struct fpm_http_gateway_s *gw)
{
	while (!TAILQ_EMPTY(&gw->waiting)) {
		fpm_http_upstream *up, *idle = NULL;
		fpm_http_conn *c;

		TAILQ_FOREACH(up, &gw->upstreams, link) {
			if (!up->busy) {
				idle = up;
				break;
			}
		}
		if (!idle && gw->nupstreams < gw->max_upstreams) {
			idle = fpm_http_upstream_new(gw);
			if (!idle && gw->nupstreams == 0) {
				/* cannot even open one connection: fail the whole queue instead of hanging it */
				while ((c = TAILQ_FIRST(&gw->waiting))) {
					fpm_http_finish(c, 0);
				}
				return;
			}
		}
		if (!idle) {
			return; /* everything busy, the next END_REQUEST calls us again */
		}

		c = TAILQ_FIRST(&gw->waiting);
		TAILQ_REMOVE(&gw->waiting, c, link);
		c->queued = 0;
		c->upstream = idle;
		idle->busy = 1;
		idle->current = c;
		if (idle_ms > 0 && !idle->connecting) {
			event_add(idle->ev_read, NULL);		/* drop the idle deadline for the duration of the request */
		}
		fpm_http_upstream_write(idle, ZSTR_VAL(c->out.s), ZSTR_LEN(c->out.s));
		smart_str_free(&c->out);
	}
}

/* the client went away: stop writing to it, but let the pool finish so the connection stays usable */
static void fpm_http_client_closed(struct evhttp_connection *evcon, void *arg)
{
	fpm_http_conn *c = arg;

	c->evcon = NULL;
	if (c->upstream) {
		c->upstream->current = NULL;
		c->upstream = NULL;
	}
	fpm_http_conn_free(c);
}

static void fpm_http_request(struct evhttp_request *req, void *arg)
{
	struct fpm_http_gateway_s *gw = arg;
	fpm_http_conn *c = calloc(1, sizeof(*c));
	int error;

	c->gw = gw;
	c->req = req;
	c->evcon = evhttp_request_get_connection(req);

	if ((error = fpm_http_build_request(c))) {
		evhttp_send_error(req, error, NULL);
		c->evcon = NULL;
		fpm_http_conn_free(c);
		return;
	}

	evhttp_connection_set_closecb(c->evcon, fpm_http_client_closed, c);
	TAILQ_INSERT_TAIL(&gw->waiting, c, link);
	c->queued = 1;
	fpm_http_pump(gw);
}

/* ---------------------------------------------------------------- processes */

static int fpm_http_resolve_upstream(struct fpm_http_gateway_s *gw)
{
	const char *address = gw->listen_address;

	if (fpm_sockets_domain_from_address(gw->listen_address) == FPM_AF_UNIX) {
		struct sockaddr_un *sa_un = (struct sockaddr_un*)&gw->upstream_addr;

		sa_un->sun_family = AF_UNIX;
		strlcpy(sa_un->sun_path, address, sizeof(sa_un->sun_path));
		gw->upstream_len = sizeof(*sa_un);
		return 0;
	} else {
		struct addrinfo hints, *res;
		char *dup_address = strdup(address), *host = NULL, *port = strrchr(dup_address, ':');
		int ret;

		if (port) {
			*port++ = '\0';
			host = dup_address;
			if (host[0] == '[' && host[strlen(host) - 1] == ']') {
				host[strlen(host) - 1] = '\0';
				host++;
			}
		} else {
			port = dup_address; /* a bare port listens on any address */
		}
		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		ret = getaddrinfo(host ? host : "localhost", port, &hints, &res);
		if (ret == 0) {
			memcpy(&gw->upstream_addr, res->ai_addr, res->ai_addrlen);
			gw->upstream_len = res->ai_addrlen;
			freeaddrinfo(res);
		}
		free(dup_address);
		return ret == 0 ? 0 : -1;
	}
}

static void fpm_http_gateway_run(struct fpm_http_gateway_s *gw, unsigned index) /* {{{ */
{
	struct fpm_worker_pool_s *wp;
	struct sigaction act;
	char title[128];

	fpm_globals.is_child = 1;

	/* plain defaults: the master terminates us with a signal, nothing to clean up */
	memset(&act, 0, sizeof(act));
	act.sa_handler = SIG_DFL;
	sigaction(SIGTERM, &act, 0);
	sigaction(SIGINT, &act, 0);
	sigaction(SIGQUIT, &act, 0);
	sigaction(SIGUSR1, &act, 0);
	sigaction(SIGUSR2, &act, 0);
	sigaction(SIGCHLD, &act, 0);
	act.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &act, 0);
	fpm_signals_unblock();

	/* the pools' FastCGI listeners are the master's business */
	for (wp = fpm_worker_all_pools; wp; wp = wp->next) {
		close(wp->listening_socket);
	}

	snprintf(title, sizeof(title), "http gateway %s [%u]", gw->pool, index);
	fpm_env_setproctitle(title);

	if (gw->reuseport) {
		/* own listening socket in the SO_REUSEPORT group, the kernel spreads connections by hash */
		close(gw->listen_fd);
		gw->listen_fd = fpm_http_listen(gw->pool, gw->listen_address, gw->backlog, 1);
		if (gw->listen_fd < 0) {
			exit(FPM_EXIT_SOFTWARE);
		}
	}

	if (fpm_http_resolve_upstream(gw) != 0) {
		zlog(ZLOG_ERROR, "[pool %s] http: cannot resolve '%s'", gw->pool, gw->listen_address);
		exit(FPM_EXIT_SOFTWARE);
	}
	TAILQ_INIT(&gw->upstreams);
	TAILQ_INIT(&gw->waiting);

	gw->base = event_base_new();
	gw->http = evhttp_new(gw->base);
	evhttp_set_allowed_methods(gw->http, EVHTTP_REQ_GET | EVHTTP_REQ_POST | EVHTTP_REQ_HEAD | EVHTTP_REQ_PUT |
		EVHTTP_REQ_DELETE | EVHTTP_REQ_OPTIONS | EVHTTP_REQ_PATCH);
	evhttp_set_max_body_size(gw->http, FPM_HTTP_MAX_BODY);
	evhttp_set_gencb(gw->http, fpm_http_request, gw);
	evutil_make_socket_nonblocking(gw->listen_fd);
	if (evhttp_accept_socket(gw->http, gw->listen_fd) != 0) {
		zlog(ZLOG_ERROR, "[pool %s] http: evhttp_accept_socket() failed", gw->pool);
		exit(FPM_EXIT_SOFTWARE);
	}

	event_base_dispatch(gw->base);
	exit(FPM_EXIT_OK);
}
/* }}} */

/* Listen on the FastCGI address with the port bumped by one. Returns -1 when that is not possible. */
static int fpm_http_listen(const char *pool, const char *listen_address, int backlog, int reuseport) /* {{{ */
{
	char *dup_address = strdup(listen_address), *host = NULL, *port_str = strrchr(dup_address, ':');
	char port[sizeof("65535")];
	struct addrinfo hints, *res, *p;
	int fd = -1, port_no, on = 1;

	if (port_str) {
		*port_str++ = '\0';
		host = dup_address;
		if (host[0] == '[' && host[strlen(host) - 1] == ']') {
			host[strlen(host) - 1] = '\0';
			host++;
		}
	} else {
		port_str = dup_address;
	}
	port_no = atoi(port_str) + 1;
	if (port_no > 65535) {
		zlog(ZLOG_WARNING, "[pool %s] no HTTP listener: no port left above '%s'", pool, listen_address);
		free(dup_address);
		return -1;
	}
	snprintf(port, sizeof(port), "%d", port_no);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	if (getaddrinfo(host, port, &hints, &res) != 0) {
		zlog(ZLOG_WARNING, "[pool %s] no HTTP listener: cannot resolve '%s'", pool, listen_address);
		free(dup_address);
		return -1;
	}
	for (p = res; p && fd < 0; p = p->ai_next) {
		fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (fd < 0) {
			continue;
		}
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
#ifdef SO_REUSEPORT
		if (reuseport) {
			setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
		}
#endif
		if (bind(fd, p->ai_addr, p->ai_addrlen) != 0 || listen(fd, backlog) != 0) {
			zlog(ZLOG_WARNING, "[pool %s] no HTTP listener: unable to listen on %s:%s: %s", pool, host ? host : "*", port, strerror(errno));
			close(fd);
			fd = -1;
			break;
		}
		fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
	}
	freeaddrinfo(res);
	free(dup_address);
	return fd;
}
/* }}} */

static void fpm_http_cleanup(int which, void *arg) /* {{{ */
{
	struct fpm_http_gateway_s *gw, *next;
	unsigned i;

	for (gw = gateways; gw; gw = next) {
		next = gw->next;
		for (i = 0; i < gw->nproc; i++) {
			if (gw->pids[i] > 0) {
				kill(gw->pids[i], SIGTERM);
			}
		}
		for (i = 0; i < gw->nproc; i++) {
			if (gw->pids[i] > 0) {
				waitpid(gw->pids[i], NULL, 0);
			}
		}
		if (gw->listen_fd >= 0) {
			close(gw->listen_fd);
		}
		free(gw->pids);
		free(gw->pool);
		free(gw->listen_address);
		free(gw->docroot);
		free(gw);
	}
	gateways = NULL;
}
/* }}} */

int fpm_http_init_main(void) /* {{{ */
{
	struct fpm_worker_pool_s *wp;
	char cwd[MAXPATHLEN];
	const char *env = getenv("FPM_HTTP_GATEWAYS");
	unsigned nproc_wanted = env && atoi(env) > 0 ? (unsigned)atoi(env) : FPM_HTTP_GATEWAYS_DEFAULT;
	int reuseport = getenv("FPM_HTTP_REUSEPORT") && atoi(getenv("FPM_HTTP_REUSEPORT")) > 0;
	const char *idle_env = getenv("FPM_HTTP_IDLE_MS");

	if (idle_env) {
		idle_ms = atoi(idle_env);
		idle_timeout.tv_sec = idle_ms / 1000;
		idle_timeout.tv_usec = (idle_ms % 1000) * 1000;
	}

	if (!getcwd(cwd, sizeof(cwd))) {
		strcpy(cwd, "/");
	}

	for (wp = fpm_worker_all_pools; wp; wp = wp->next) {
		struct fpm_http_gateway_s *gw;
		unsigned workers = wp->config->pm_max_children > 0 ? (unsigned)wp->config->pm_max_children : 1;
		unsigned i;

		if (wp->listen_address_domain != FPM_AF_INET) {
			continue;
		}

		gw = calloc(1, sizeof(*gw));
		gw->pool = strdup(wp->config->name);
		gw->listen_address = strdup(wp->config->listen_address);
		gw->docroot = strdup(wp->config->chdir && *wp->config->chdir ? wp->config->chdir : cwd);
		gw->backlog = wp->config->listen_backlog;
		gw->reuseport = reuseport;
		gw->listen_fd = fpm_http_listen(gw->pool, gw->listen_address, gw->backlog, reuseport);
		if (gw->listen_fd < 0) {
			free(gw->pool);
			free(gw->listen_address);
			free(gw->docroot);
			free(gw);
			continue;
		}
		/* every persistent connection pins a worker, so the gateways share the workers between them */
		gw->nproc = MIN(nproc_wanted, workers);
		gw->pids = calloc(gw->nproc, sizeof(pid_t));
		gw->next = gateways;
		gateways = gw;
		zlog(ZLOG_NOTICE, "[pool %s] HTTP listener on FastCGI port + 1: %u gateway(s)%s, %u persistent connection(s) to the pool",
			wp->config->name, gw->nproc, reuseport ? " with SO_REUSEPORT" : "", workers);

		for (i = 0; i < gw->nproc; i++) {
			gw->pids[i] = fork();
			if (gw->pids[i] < 0) {
				zlog(ZLOG_SYSERROR, "[pool %s] http: fork() failed", wp->config->name);
			} else if (gw->pids[i] == 0) {
				gw->max_upstreams = workers / gw->nproc + (i < workers % gw->nproc ? 1 : 0);
				fpm_http_gateway_run(gw, i);
				/* not reached */
			}
		}
		if (reuseport) {
			/* the master's socket would otherwise take its share of connections and never accept them */
			close(gw->listen_fd);
			gw->listen_fd = -1;
		}
	}

	if (0 > fpm_cleanup_add(FPM_CLEANUP_PARENT, fpm_http_cleanup, 0)) {
		return -1;
	}
	return 0;
}
/* }}} */

#else /* HAVE_FPM_HTTP */

int fpm_http_init_main(void)
{
	return 0;
}

#endif
