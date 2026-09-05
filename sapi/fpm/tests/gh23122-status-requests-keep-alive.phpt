--TEST--
FPM: GH-23122 request counters must not count reads that serve no request
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

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();

for ($i = 0; $i < 3; $i++) {
    $tester->request(connKeepAlive: true)->expectEmptyBody();
}
usleep(200000);

$body = $tester->request('full', [], '/status')->getBody('text/plain');
if ($body === null) {
    $tester->printLogs();
    die("no status body\n");
}

preg_match('/^accepted conn:\s+(\d+)/m', $body, $m);
echo "accepted conn: ", ($m[1] === '4' ? 'ok' : "WRONG {$m[1]}, expected 4"), "\n";

preg_match_all('/^requests:\s+(\d+)/m', $body, $m);
$perWorker = array_map('intval', $m[1]);
sort($perWorker);
echo "per worker requests: ", ($perWorker === [1, 3] ? 'ok' : 'WRONG ' . implode(',', $perWorker) . ', expected 1,3'), "\n";

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
accepted conn: ok
per worker requests: ok
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
