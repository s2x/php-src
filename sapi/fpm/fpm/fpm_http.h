#ifndef FPM_HTTP_H
#define FPM_HTTP_H 1

/* Plain HTTP gateway processes, one set per TCP pool (see fpm_http.c). */
int fpm_http_init_main(void);

#endif
