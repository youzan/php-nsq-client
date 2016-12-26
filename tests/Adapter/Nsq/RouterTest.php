<?php
/**
 * Router test
 * User: moyo
 * Date: 25/12/2016
 * Time: 2:04 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Router;
use Kdt\Iron\Queue\Exception\ShardingStrategyException;
use Kdt\Iron\Queue\Tests\classes\nsqphp\HTTP;

use Exception;

class RouterTest extends \PHPUnit_Framework_TestCase
{
    public function testPublishNodes()
    {
        $topic = 'router_topic_biz';

        $gotNodes = Router::getInstance()->fetchPublishNodes($topic);

        $expectInfo = [
            ['host' => '127.0.0.1', 'ports' => ['tcp' => 31, 'http' => 31]],
            ['host' => '127.0.0.1', 'ports' => ['tcp' => 32, 'http' => 32]],
        ];

        $this->assertArraySubset($expectInfo, $gotNodes, TRUE);
    }

    public function testSubscribeNodes()
    {
        $topic = 'router_topic_biz';

        $gotNodes = Router::getInstance()->fetchSubscribeNodes($topic);

        $expectInfo = [
            ['host' => '127.0.0.1', 'ports' => ['tcp' => 33, 'http' => 33]],
            ['host' => '127.0.0.1', 'ports' => ['tcp' => 34, 'http' => 34]],
        ];

        $this->assertArraySubset($expectInfo, $gotNodes, TRUE);
    }

    public function testSubscribeNodesCustomPartitionSuccess()
    {
        $topic = 'sharding_with_enabled';

        $gotNodes = Router::getInstance()->fetchSubscribeNodes($topic, 1);

        $expectInfo = [
            ['host' => '127.0.0.1', 'ports' => ['tcp' => 3, 'http' => 3]],
        ];

        $this->assertArraySubset($expectInfo, $gotNodes, TRUE);
    }

    public function testSubscribeNodesCustomPartitionMissing()
    {
        $topic = 'sharding_with_enabled';
        $expectMsg = 'Custom partition not found';

        $exceptionGot = null;
        try
        {
            Router::getInstance()->fetchSubscribeNodes($topic, 2);
        }
        catch (Exception $e)
        {
            $exceptionGot = $e;
        }

        $this->assertInstanceOf(ShardingStrategyException::class, $exceptionGot);
        $this->assertEquals($expectMsg, $exceptionGot->getMessage());
    }

    public function testPublishViaType()
    {
        // need "http" for mocking
        $this->assertEquals('http', Router::getInstance()->fetchPublishViaType());
    }

    public function testClearCaches()
    {
        $topic = 'router_topic_biz';

        $reqCountInit = HTTP::reqCount();

        // begin s1
        Router::getInstance()->fetchPublishNodes($topic);
        $reqCountS1 = HTTP::reqCount();

        // s1 will use cache, no http req
        $this->assertEquals($reqCountInit, $reqCountS1);

        // clear cache
        Router::getInstance()->clearCaches();

        // begin s2
        Router::getInstance()->fetchPublishNodes($topic);
        $reqCountS2 = HTTP::reqCount();

        // s2 will do one http req for lookup a topic
        $this->assertEquals($reqCountInit + 1, $reqCountS2);
    }
}