<?php
/**
 * ConnectionPool test
 * User: moyo
 * Date: 29/12/2016
 * Time: 3:32 PM
 */

namespace Kdt\Iron\Queue\Tests\nsqphp\Connection;

use nsqphp\Connection\Connection;
use nsqphp\Connection\ConnectionPool;

class ConnectionPoolTest extends \PHPUnit_Framework_TestCase
{
    public function testGetInstance()
    {
        $ins1 = ConnectionPool::getInstance('test');
        $ins2 = ConnectionPool::getInstance('test');

        $this->assertEquals(spl_object_hash($ins1), spl_object_hash($ins2));
    }

    public function testOperate()
    {
        $conn1 = new Connection('host1', 1, 2);
        $conn2 = new Connection('host2', 3, 4);

        $pool = ConnectionPool::getInstance('test');

        // test add

        $this->assertEquals(0, count($pool));
        $pool->add($conn1);
        $pool->add($conn2);
        $this->assertEquals(2, count($pool));

        // hasConnection and findConnection used real socket so can't test now

        // test hasHost

        $this->assertEquals(true, $pool->hasHost('host1', 1, 2));
        $this->assertEquals(true, $pool->hasHost('host2', 3, 4));
        $this->assertEquals(false, $pool->hasHost('host3', 1, 2));
        $this->assertEquals(false, $pool->hasHost('host3', 3, 4));

        // test filterConnections

        $expectHost = ['host' => 'host1', 'ports' => ['tcp' => 1, 'http' => 2], 'partition' => null];

        $filters = $pool->filterConnections([$expectHost]);

        $this->assertEquals(1, count($filters));

        $expectIdx = 'host1:1/2/-N';
        $filterNode = current($filters);
        $this->assertEquals($expectIdx, (string)$filterNode);

        // test emptyConnections

        $this->assertEquals(2, count($pool));
        $pool->emptyConnections();

        // ~ test connectionIndex

        $filters2 = $pool->filterConnections([$expectHost]);
        $this->assertEquals(0, count($filters2));

        $this->assertEquals(0, count($pool));
    }
}