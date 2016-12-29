<?php
/**
 * Queue test
 * User: moyo
 * Date: 29/12/2016
 * Time: 5:27 PM
 */

namespace Kdt\Iron\Queue\Tests;

use Kdt\Iron\Queue\Queue;

class QueueTest extends \PHPUnit_Framework_TestCase
{
    public function testPushOK()
    {
        $topic = 'queue.push.normal';
        $msg = 'hello world';

        $result = Queue::push($topic, $msg);

        $this->assertEquals(true, $result);
    }

    public function testPushFailed()
    {
        $topic = 'queue.push.failed';
        $msg = 'hello hell';
        $reason = 'nsqphp\Exception\FailedOnAllNodesException : door closed';

        $result = Queue::push($topic, $msg, ['max_retry' => 1, 'retry_delay_ms' => 0]);

        $this->assertEquals(false, $result);
        $this->assertEquals($reason, Queue::lastPushError());
    }
}