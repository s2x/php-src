--TEST--
FPM: request_terminate_timeout must not apply to a worker waiting on a kept alive connection
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
request_terminate_timeout = 1
EOT;

function getPids(FPM\Tester $tester): array
{
    $body = (string) $tester->request('full', [], '/status')->getBody('text/plain');
    preg_match_all('/^pid:\s+(\d+)/m', $body, $matches);
    $pids = $matches[1];
    sort($pids);
    return $pids;
}

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();

$tester->request(connKeepAlive: true)->expectEmptyBody();
usleep(100000);
$pidsBefore = getPids($tester);

sleep(3);

$pidsAfter = getPids($tester);
echo $pidsBefore === $pidsAfter ? "workers unchanged\n" : "workers changed\n";

$tester->request(connKeepAlive: true)->expectEmptyBody();

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->expectNoLogPattern('/execution timed out/');
$tester->close();

?>
Done
--EXPECT--
workers unchanged
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
