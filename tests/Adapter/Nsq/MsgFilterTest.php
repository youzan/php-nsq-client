<?php
/**
 * Msg filter test
 * User: moyo
 * Date: 23/12/2016
 * Time: 3:22 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\MsgFilter;
use Kdt\Iron\Queue\Message as QMessage;

class MsgFilterTest extends \PHPUnit_Framework_TestCase
{
    public function testNormalProcess()
    {
        $topic = 'filter_topic_normal';

        $nsqMsg = MsgFilter::getInstance()->getMsgObject($topic, 'hello-world');

        $this->assertEquals('"hello-world"', $nsqMsg->getPayload());
        $this->assertEquals(null, $nsqMsg->getTraceId());
        $this->assertEquals(null, $nsqMsg->getLimitedNode());
    }

    public function testShardingProcess()
    {
        $topic = 'sharding_topic_1';

        $shardingProof = 3;
        $mockingPartitionNum = 2;
        $expectPartitionID = (string)($shardingProof % $mockingPartitionNum);

        $bizMsg = new QMessage('msg-data');
        $bizMsg->setShardingProof(3);

        $nsqMsg = MsgFilter::getInstance()->getMsgObject($topic, $bizMsg);

        $limitedNode = $nsqMsg->getLimitedNode();

        $this->assertEquals($expectPartitionID, $limitedNode['partition']);
    }
}