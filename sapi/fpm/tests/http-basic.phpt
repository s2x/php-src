--TEST--
FPM: plain HTTP gateway next to the FastCGI socket (--with-fpm-http)
--SKIPIF--
<?php
include "skipif.inc";
if (!str_contains((string) shell_exec(FPM\Tester::findExecutable() . ' -n -i 2>/dev/null'), 'http gateway')) {
    die('skip php-fpm built without --with-fpm-http');
}
?>
--FILE--
<?php

require_once "tester.inc";

$dir = __DIR__;
$cfg = <<<EOT
[global]
error_log = {{FILE:LOG}}
[unconfined]
listen = {{ADDR}}
pm = static
pm.max_children = 1
chdir = $dir
EOT;

$code = <<<'EOT'
<?php
header('X-Test: fpm-http');
echo $_SERVER['REQUEST_METHOD'], ' ', basename($_SERVER['SCRIPT_NAME']), ' ',
    $_SERVER['PATH_INFO'] ?? '-', ' ', $_SERVER['QUERY_STRING'], ' ', $_SERVER['HTTP_X_IN'] ?? '-', "\n";
echo 'body=', file_get_contents('php://input'), "\n";
EOT;

$tester = new FPM\Tester($cfg, $code);
$tester->start();
$tester->expectLogStartNotices();

$script = basename($tester->makeSourceFile());
[$host, $port] = explode(':', $tester->getAddr());
$base = "http://$host:" . ($port + 1); // HTTP listens on the FastCGI port + 1

echo file_get_contents("$base/$script/extra%20info?x=1&y=2", false, stream_context_create(['http' => [
    'header' => "X-In: hello\r\n",
]]));
$headers = http_get_last_response_headers();
var_dump($headers[0], in_array('X-Test: fpm-http', $headers, true));

echo file_get_contents("$base/$script", false, stream_context_create(['http' => [
    'method'  => 'POST',
    'header'  => "Content-Type: application/x-www-form-urlencoded\r\n",
    'content' => 'a=1&b=2',
]]));

file_get_contents("$base/missing.php", false, stream_context_create(['http' => ['ignore_errors' => true]]));
var_dump(http_get_last_response_headers()[0]);

// FastCGI keeps working on its own socket
$tester->request()->expectBody("GET $script -  -\nbody=");

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECTF--
GET %s.src.php /extra info x=1&y=2 hello
body=
string(15) "HTTP/1.1 200 OK"
bool(true)
POST %s.src.php -  -
body=a=1&b=2
string(22) "HTTP/1.1 404 Not Found"
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
