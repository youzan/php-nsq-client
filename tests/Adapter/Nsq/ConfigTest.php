<?php
/**
 * Config test
 * User: moyo
 * Date: 23/12/2016
 * Time: 11:16 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Config;
use Kdt\Iron\Queue\Exception\InvalidConfigException;

class ConfigTest extends \PHPUnit_Framework_TestCase
{
    public function testParseTopicName()
    {
        $names = [
            'config_topic_same'  => 'config_topic_same',
            'config.topic.doted' => 'config_topic_doted',
            'config.topic+chars' => 'config_topic+chars',
            'config_topic_name'  => 'config_topic_name_full'
        ];

        foreach ($names as $named => $parsed)
        {
            $this->assertEquals($parsed, Config::getInstance()->parseTopicName($named));
        }
    }

    public function testGetTopicConfigException()
    {
        $eNotFound = null;
        try
        {
            Config::getInstance()->getTopicConfig('404.this.topic.not.exists');
        }
        catch (\Exception $e)
        {
            $eNotFound = $e;
        }
        $this->assertInstanceOf(InvalidConfigException::class, $eNotFound);
        $this->assertEquals(9997, $eNotFound->getCode());

        $eEmptyFound = null;
        try
        {
            Config::getInstance()->getTopicConfig('empty.this.topic.empty');
        }
        catch (\Exception $e)
        {
            $eEmptyFound = $e;
        }
        $this->assertInstanceOf(InvalidConfigException::class, $eEmptyFound);
        $this->assertEquals(9998, $eEmptyFound->getCode());
    }

    public function testGetTopicConfigNormal()
    {
        $topicConfig = Config::getInstance()->getTopicConfig('config.topic_name');
        $configExpect = [
            'group' => 'config',
            'scope' => 'config',
            'name' => 'config_topic_name',
            'topic' => 'config_topic_name_full',
            'lookups' => [
                'r' => ['lookupd-default' => '@self'],
                'w' => ['lookupd-default' => '@self'],
            ]
        ];
        $this->assertArraySubset($configExpect, $topicConfig);
    }

    public function testGetTopicConfigStrategy()
    {
        $expectMaps = [
            'strategy_cluster_default0' => ['r' => 'lookupd-c', 'w' => 'lookupd-c'],
            'strategy_cluster_only_a_0' => ['r' => 'lookupd-a', 'w' => 'lookupd-a'],
            'strategy_cluster_only_b_r' => ['r' => 'lookupd-b'],
            'strategy_cluster_r_c_r_b'  => ['r' => 'lookupd-c,lookupd-b'],
            'strategy_cluster_r_a_rw_b' => ['r' => 'lookupd-a,lookupd-b', 'w' => 'lookupd-b'],
            'strategy_cluster_w_b_rw_a' => ['r' => 'lookupd-a', 'w' => 'lookupd-b,lookupd-a'],
            'strategy_cluster_rw_c_r_a_w_b' => ['r' => 'lookupd-c,lookupd-a', 'w' => 'lookupd-c,lookupd-b'],
        ];

        foreach ($expectMaps as $topic => $expectResult)
        {
            $expectLookups = [];
            $pipes = ['r', 'w'];
            foreach ($pipes as $pipe)
            {
                if (isset($expectResult[$pipe]))
                {

                    $clusters = explode(',', $expectResult[$pipe]);
                    array_walk($clusters, function ($cluster) use (&$expectLookups, $pipe) {
                        $expectLookups[$pipe][$cluster] = '@self';
                    });
                }
            }

            $config = Config::getInstance()->getTopicConfig($topic);
            $this->assertEquals($topic == 'strategy_cluster_default0' ? 'strategy' : $topic, $config['scope']);
            $this->assertArraySubset($expectLookups, $config['lookups']);
        }
    }

    public function testGetGlobalSetting()
    {
        $this->assertEquals('hi', Config::getInstance()->getGlobalSetting('nsq.testing.config.get'));
    }
}