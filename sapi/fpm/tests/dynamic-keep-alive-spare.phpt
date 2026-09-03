--TEST--
FPM: dynamic must spawn a spare worker while the only worker waits on a kept alive connection
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
pm = dynamic
pm.max_children = 3
pm.start_servers = 1
pm.min_spare_servers = 1
pm.max_spare_servers = 1
pm.status_path = /status
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

$pids = [];
for ($i = 0; $i < 30; $i++) {
    $pids = getPids($tester);
    if (count($pids) === 2) {
        break;
    }
    usleep(100000);
}
echo count($pids) === 2 ? "spare worker spawned\n" : "no spare worker spawned\n";

sleep(2);
echo getPids($tester) === $pids ? "workers unchanged\n" : "workers changed\n";

$start = microtime(true);
$tester->request()->expectEmptyBody();
echo "new connection served: ", microtime(true) - $start < 1.0 ? 'promptly' : 'LATE', "\n";

$tester->request(connKeepAlive: true)->expectEmptyBody();

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
spare worker spawned
workers unchanged
new connection served: promptly
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
