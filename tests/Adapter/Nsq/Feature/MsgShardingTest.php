<?php
/**
 * Msg sharding test
 * User: moyo
 * Date: 22/12/2016
 * Time: 5:50 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq\Feature;

use Kdt\Iron\Queue\Adapter\Nsq\Feature\MsgSharding;
use Kdt\Iron\Queue\Message as QMessage;
use nsqphp\Message\Message as NSQMessage;

class MsgShardingTest extends \PHPUnit_Framework_TestCase
{
    public function testProcess()
    {
        $topic = 'sharding_topic_1';

        $shardingProof = 3;
        $mockingPartitionNum = 2;
        $expectPartitionID = (string)($shardingProof % $mockingPartitionNum);

        $origin = new QMessage('msg-data');
        $origin->setShardingProof(3);

        $target = new NSQMessage($origin->getPayload());

        MsgSharding::getInstance()->process($topic, $origin, $target);

        $limitedNode = $target->getLimitedNode();

        $this->assertEquals($expectPartitionID, $limitedNode['partition']);
    }
}