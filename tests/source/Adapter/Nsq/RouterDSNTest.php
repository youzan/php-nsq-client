<?php
/**
 * Router test for dsn/lookups
 * User: moyo
 * Date: 25/12/2016
 * Time: 2:07 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Router;

class RouterDSNTest extends \PHPUnit_Framework_TestCase
{
    public function testTranslateSyntax()
    {
        $lookups = Router::getInstance()->fetchGlobalLookups('dsn_topic_syntax_old');
        $expect = [
            'r' => ['lookupd-dsn-syntax-old' => ['http://127.0.0.1:2']],
            'w' => ['lookupd-dsn-syntax-old' => ['http://127.0.0.1:2']],
        ];

        $this->assertArraySubset($lookups, $expect, TRUE);
    }

    public function testTranslateNormal()
    {
        $lookups = Router::getInstance()->fetchGlobalLookups('dsn_topic_normal');
        $expect = [
            'r' => ['lookupd-dsn-normal' => ['http://127.0.0.1:2']],
            'w' => ['lookupd-dsn-normal' => ['http://127.0.0.1:2']],
        ];

        $this->assertArraySubset($lookups, $expect, TRUE);
    }

    public function testTranslateBalanced()
    {
        $lookups = Router::getInstance()->fetchGlobalLookups('dsn_topic_balanced');
        $avaLookups = ['http://127.0.0.1:2', 'http://127.0.0.1:3'];
        $clusterName = 'lookupd-dsn-balanced';

        $this->assertTrue(in_array(current($lookups['r'][$clusterName]), $avaLookups));
        $this->assertTrue(in_array(current($lookups['w'][$clusterName]), $avaLookups));
    }

    public function testTranslateDiscovery()
    {
        $lookups = Router::getInstance()->fetchGlobalLookups('dsn_topic_discovery');
        $clusterName = 'lookupd-dsn-discovery';
        $dynLookups = ['http://127.0.0.2:11', 'http://127.0.0.2:22'];
        $expect = [
            'r' => [$clusterName => $dynLookups],
            'w' => [$clusterName => $dynLookups],
        ];

        $this->assertArraySubset($lookups, $expect, TRUE);
    }
}