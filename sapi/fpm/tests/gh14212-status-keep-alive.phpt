--TEST--
FPM: GH-14212 process counts must not exceed pm.max_children on keep alive connections
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

$closeKeepAliveClients = function () {
    foreach ($this->clients as $clientsByPort) {
        foreach ($clientsByPort as $client) {
            (function () { $this->transport->close(); })->call($client);
        }
    }
    $this->clients = [];
};

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();
$tester->status($expectedStatusData);

for ($i = 0; $i < 3; $i++) {
    $tester->request(connKeepAlive: true)->expectEmptyBody();
    usleep(100000);
    $tester->status($expectedStatusData);
}

for ($i = 0; $i < 3; $i++) {
    $tester->request(connKeepAlive: true)->expectEmptyBody();
    $closeKeepAliveClients->call($tester);
    usleep(100000);
    $tester->status($expectedStatusData);
}

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
