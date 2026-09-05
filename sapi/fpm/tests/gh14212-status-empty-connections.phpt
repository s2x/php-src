--TEST--
FPM: GH-14212 connections that carry no request must not change process counts
--SKIPIF--
<?php include "skipif.inc"; ?>
--FILE--
<?php

require_once "tester.inc";

$cfg = <<<EOT
[global]
error_log = {{FILE:LOG}}
[unconfined]
listen = {{ADDR}}
pm = static
pm.max_children = 2
pm.status_path = /status
EOT;

$expectedStatusData = [
    'process manager'      => 'static',
    'total processes'      => 2,
    'max children reached' => 0,
];

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();
$tester->status($expectedStatusData);

for ($i = 0; $i < 5; $i++) {
    $socket = stream_socket_client('tcp://' . $tester->getAddr());
    if ($socket === false) {
        die("failed to connect\n");
    }
    fclose($socket);
    usleep(20000);
}
usleep(100000);
$tester->status($expectedStatusData);

for ($i = 0; $i < 3; $i++) {
    $tester->requestValues()->expectValue('FCGI_MPXS_CONNS', '0');
}
usleep(100000);
$tester->status($expectedStatusData);

$tester->requestValues(connKeepAlive: true)->expectValue('FCGI_MPXS_CONNS', '0');
$tester->request(connKeepAlive: true)->expectEmptyBody();
$tester->requestValues(connKeepAlive: true)->expectValue('FCGI_MPXS_CONNS', '0');
$tester->request(connKeepAlive: true)->expectEmptyBody();
usleep(100000);
$tester->status($expectedStatusData);

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
