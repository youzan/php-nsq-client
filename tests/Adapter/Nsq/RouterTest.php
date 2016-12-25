<?php
/**
 * Router test
 * User: moyo
 * Date: 25/12/2016
 * Time: 2:04 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Router;

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

    public function testPublishViaType()
    {

    }

    public function testClearCaches()
    {

    }
}