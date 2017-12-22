<?php
/**
 * Connection test
 * User: moyo
 * Date: 29/12/2016
 * Time: 2:39 PM
 */

namespace Kdt\Iron\Queue\Tests\nsqphp\Connection;

use nsqphp\Connection\Connection;
use nsqphp\Connection\Proxy;

class ConnectionTest extends \PHPUnit_Framework_TestCase
{
    public function testInitialize()
    {
        $host = '127.0.0.1';
        $portTCP = 5;
        $portHTTP = 6;
        $cluster = true;
        $partition = 1;
        $connType = 'tcp';

        $connNew = new Connection($host, $portTCP, $portHTTP, $cluster, $partition, $connType);
        $connOld = new Connection($host, $portTCP, $portHTTP, false, null, $connType);

        // base test

        $this->assertEquals($cluster, $connNew->isYzCluster());
        $this->assertEquals($partition, $connNew->getPartitionID());
        $this->assertEquals($connType, $connNew->getConnType());

        $this->assertEquals(false, $connNew->isOrderedSub());
        $connNew->setOrderedSub();
        $this->assertEquals(true, $connNew->isOrderedSub());

        $idxNew = "{$host}:{$portTCP}/{$portHTTP}/-{$partition}";
        $this->assertEquals($idxNew, (string)$connNew);

        $idxOld = "{$host}:{$portTCP}/{$portHTTP}/-N";
        $this->assertEquals($idxOld, (string)$connOld);

        // http post

        $connNew->setProxy(new Proxy());

        $result = $connNew->post('/postAPITest', 'post-body');

        $this->assertEquals('"success for mock"', $result);
    }
}