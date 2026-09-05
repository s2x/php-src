--TEST--
FPM: pm.max_requests recycling a worker on a kept alive connection keeps the counters right
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
pm.max_requests = 2
pm.status_path = /status
EOT;

$statusRequests = 0;

function getStatus(FPM\Tester $tester): array
{
    global $statusRequests;
    $statusRequests++;
    $body = (string) $tester->request('full', [], '/status')->getBody('text/plain');
    preg_match_all('/^pid:\s+(\d+)/m', $body, $matches);
    $pids = $matches[1];
    sort($pids);
    preg_match('/^accepted conn:\s+(\d+)/m', $body, $matches);
    $accepted = (int) $matches[1];
    preg_match('/^total processes:\s+(\d+)/m', $body, $matches);
    return [$pids, $accepted, (int) $matches[1]];
}

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();
[$pidsBefore] = getStatus($tester);

$tester->request(connKeepAlive: true)->expectEmptyBody();
$tester->request(connKeepAlive: true)->expectEmptyBody();

$recycled = false;
for ($i = 0; $i < 30; $i++) {
    usleep(100000);
    [$pids] = getStatus($tester);
    if ($pids !== $pidsBefore && count($pids) === count($pidsBefore)) {
        $recycled = true;
        break;
    }
}
echo $recycled ? "worker recycled\n" : "worker not recycled\n";

usleep(1100000);
[, $accepted, $total] = getStatus($tester);
echo "total processes: ", $total === 2 ? 'ok' : "WRONG $total", "\n";
$expectedAccepted = 2 + $statusRequests;
echo "accepted conn: ", $accepted === $expectedAccepted ? 'ok' : "WRONG $accepted, expected $expectedAccepted", "\n";

(function () { $this->clients = []; })->call($tester);
$tester->request(connKeepAlive: true)->expectEmptyBody();

$tester->terminate();
$tester->expectLogNotice('child %d exited with code 0 after %s seconds from start', 'unconfined');
$tester->expectLogNotice('child %d started', 'unconfined');
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
worker recycled
total processes: ok
accepted conn: ok
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
