<?php
/**
 * Msg filter test
 * User: moyo
 * Date: 23/12/2016
 * Time: 3:22 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\MsgFilter;
use Kdt\Iron\Queue\Exception\ShardingStrategyException;
use Kdt\Iron\Queue\Message as QMessage;

use Exception;

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

    public function testTraceProcess()
    {
        $traceID = rand(0, 999);

        $trMap = [
            'filter_topic_normal' => null,
            'filter_topic_traced' => $traceID,
        ];

        foreach ($trMap as $topic => $expectResult)
        {
            $bizMsg = new QMessage('msg-data');
            $bizMsg->setTraceID($traceID);

            $nsqMsg = MsgFilter::getInstance()->getMsgObject($topic, $bizMsg);

            $this->assertEquals($expectResult, $nsqMsg->getTraceId());
        }
    }

    public function testShardingEnabled()
    {
        $topic = 'sharding_with_enabled';

        $shardingProof = 3;
        $mockingPartitionNum = 2;
        $expectPartitionID = (string)($shardingProof % $mockingPartitionNum);

        $bizMsg = new QMessage('msg-data');
        $bizMsg->setShardingProof(3);

        $nsqMsg = MsgFilter::getInstance()->getMsgObject($topic, $bizMsg);

        $limitedNode = $nsqMsg->getLimitedNode();

        $this->assertEquals($expectPartitionID, $limitedNode['partition']);
    }

    public function testShardingWithoutProof()
    {
        $topic = 'sharding_with_enabled';
        $expectMsg = 'Missing proof for sharding topic';

        $exceptionGot = null;
        try
        {
            $bizMsg = new QMessage('msg-data');

            MsgFilter::getInstance()->getMsgObject($topic, $bizMsg);
        }
        catch (Exception $e)
        {
            $exceptionGot = $e;
        }

        $this->assertInstanceOf(ShardingStrategyException::class, $exceptionGot);
        $this->assertEquals($expectMsg, $exceptionGot->getMessage());
    }

    public function testShardingDisabled()
    {
        $topic = 'sharding_topic_normal';
        $expectMsg = 'This topic can not be sharding';

        $exceptionGot = null;
        try
        {
            $bizMsg = new QMessage('msg-data');
            $bizMsg->setShardingProof(123);

            MsgFilter::getInstance()->getMsgObject($topic, $bizMsg);
        }
        catch (Exception $e)
        {
            $exceptionGot = $e;
        }

        $this->assertInstanceOf(ShardingStrategyException::class, $exceptionGot);
        $this->assertEquals($expectMsg, $exceptionGot->getMessage());
    }

    public function testShardingInBag()
    {
        $topic = 'sharding_with_enabled';
        $expectMsg = 'Messages must publish one by one';

        $exceptionGot = null;
        try
        {
            $bag = [
                (new QMessage('msg-data-1'))->setShardingProof(123),
                (new QMessage('msg-data-2'))->setShardingProof(456),
            ];

            MsgFilter::getInstance()->getMsgObjectBag($topic, $bag);
        }
        catch (Exception $e)
        {
            $exceptionGot = $e;
        }

        $this->assertInstanceOf(ShardingStrategyException::class, $exceptionGot);
        $this->assertEquals($expectMsg, $exceptionGot->getMessage());
    }

    public function testShardingMissingPartition()
    {

    }

    public function testShardingEmptyPartition()
    {

    }
}