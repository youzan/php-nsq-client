<?php
/**
 * DSN test
 * User: moyo
 * Date: 23/12/2016
 * Time: 3:33 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Config;
use Kdt\Iron\Queue\Adapter\Nsq\DSN;

class DSNTest extends \PHPUnit_Framework_TestCase
{
    public function testTranslateSyntax()
    {
        $topic = 'dsn_topic_syntax_old';
        $config = Config::getInstance()->getTopicConfig($topic);

        $lookups = DSN::getInstance()->translate($config['lookups']);
        $expect = [
            'r' => ['lookupd-dsn-syntax-old' => ['http://127.0.0.1:2']],
            'w' => ['lookupd-dsn-syntax-old' => ['http://127.0.0.1:2']],
        ];

        $this->assertArraySubset($expect, $lookups);
    }

    public function testTranslateNormal()
    {
        $topic = 'dsn_topic_normal';
        $config = Config::getInstance()->getTopicConfig($topic);

        $lookups = DSN::getInstance()->translate($config['lookups']);
        $expect = [
            'r' => ['lookupd-dsn-normal' => ['http://127.0.0.1:2']],
            'w' => ['lookupd-dsn-normal' => ['http://127.0.0.1:2']],
        ];

        $this->assertArraySubset($expect, $lookups);
    }

    public function testTranslateBalanced()
    {
        $topic = 'dsn_topic_balanced';
        $config = Config::getInstance()->getTopicConfig($topic);

        $lookups = DSN::getInstance()->translate($config['lookups']);
        $avaLookups = ['http://127.0.0.1:2', 'http://127.0.0.1:3'];
        $clusterName = 'lookupd-dsn-balanced';

        $this->assertTrue(in_array(current($lookups['r'][$clusterName]), $avaLookups));
        $this->assertTrue(in_array(current($lookups['w'][$clusterName]), $avaLookups));
    }

    public function testTranslateDiscovery()
    {

    }

    public function testDCCSBegin()
    {

    }

    public function testDCCSMoving()
    {

    }

    public function testDCCSFinish()
    {

    }
}