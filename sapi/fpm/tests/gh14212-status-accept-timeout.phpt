--TEST--
FPM: GH-14212 a connection that sends nothing before the accept timeout must not change process counts
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

$socket = stream_socket_client('tcp://' . $tester->getAddr());
if ($socket === false) {
    die("failed to connect\n");
}
$closed = false;
for ($i = 0; $i < 80; $i++) {
    $read = [$socket];
    $write = $except = null;
    if (stream_select($read, $write, $except, 0, 100000) > 0 && fread($socket, 1) === '' && feof($socket)) {
        $closed = true;
        break;
    }
}
echo $closed ? "connection closed by server\n" : "connection still open\n";

$tester->status($expectedStatusData);
fclose($socket);

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
connection closed by server
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
