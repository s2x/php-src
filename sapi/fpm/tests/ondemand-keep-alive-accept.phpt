--TEST--
FPM: ondemand must still fork when a worker waits on a kept alive connection
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
pm = ondemand
pm.max_children = 3
pm.process_idle_timeout = 30
pm.status_path = /status
EOT;

$tester = new FPM\Tester($cfg);
$tester->start();
$tester->expectLogStartNotices();

$tester->request(connKeepAlive: true)->expectEmptyBody();
usleep(100000);

$start = microtime(true);
$tester->request()->expectEmptyBody();
$elapsed = microtime(true) - $start;

echo "second connection served: ", ($elapsed < 1.0 ? 'promptly' : sprintf('LATE after %.2fs', $elapsed)), "\n";

$tester->terminate();
$tester->expectLogTerminatingNotices();
$tester->close();

?>
Done
--EXPECT--
second connection served: promptly
Done
--CLEAN--
<?php
require_once "tester.inc";
FPM\Tester::clean();
?>
