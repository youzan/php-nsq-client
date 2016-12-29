<?php
/**
 * InstanceMgr test
 * User: moyo
 * Date: 26/12/2016
 * Time: 3:24 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\InstanceMgr;
use Kdt\Iron\Queue\Adapter\Nsq\Logger;
use nsqphp\Lookup\IOSplitter;
use nsqphp\nsqphp;

use Exception;

class InstanceMgrTest extends \PHPUnit_Framework_TestCase
{
    public function testGetPubInstance()
    {
        $topic = 'router_topic_biz';
        $ins = InstanceMgr::getPubInstance($topic);

        $this->assertInstanceOf(nsqphp::class, $ins);
        $this->assertNull($ins->getNsLookup());

        // check unique
        $ins2 = InstanceMgr::getPubInstance($topic);
        $this->assertEquals(spl_object_hash($ins), spl_object_hash($ins2));

        // check retrying
        InstanceMgr::cleanWhenSrvRetrying(new Exception);
        $ins3 = InstanceMgr::getPubInstance($topic);
        $this->assertNotEquals(spl_object_hash($ins2), spl_object_hash($ins3));
    }

    public function testGetSubInstance()
    {
        $topic = 'router_topic_biz';
        $ins = InstanceMgr::getSubInstance($topic);

        $this->assertInstanceOf(nsqphp::class, $ins);
        $this->assertNull($ins->getNsLookup());

        // check non-args
        $ins2 = InstanceMgr::getSubInstance();
        $this->assertEquals(spl_object_hash($ins), spl_object_hash($ins2));

        // check retrying
        InstanceMgr::cleanWhenSrvRetrying(new Exception);
        $ins3 = InstanceMgr::getSubInstance();
        $this->assertNotEquals(spl_object_hash($ins2), spl_object_hash($ins3));
    }

    public function testGetLookupInstance()
    {
        $topics = ['router_topic_biz', 'sharding_with_enabled'];
        $scenes = ['pub', 'sub'];

        foreach ($topics as $topic)
        {
            foreach ($scenes as $scene)
            {
                // get ins
                $ins1 = InstanceMgr::getLookupInstance($topic, $scene);
                // check normal
                $this->assertInstanceOf(IOSplitter::class, $ins1);
                $ins2 = InstanceMgr::getLookupInstance($topic, $scene);
                $this->assertEquals(spl_object_hash($ins1), spl_object_hash($ins2));
                // check retrying
                InstanceMgr::cleanWhenSrvRetrying(new Exception);
                $ins3 = InstanceMgr::getLookupInstance($topic, $scene);
                $this->assertNotEquals(spl_object_hash($ins2), spl_object_hash($ins3));
            }
        }
    }

    public function testGetLoggerInstance()
    {
        $this->assertInstanceOf(Logger::class, InstanceMgr::getLoggerInstance());
    }
}