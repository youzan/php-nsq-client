<?php
/**
 * Cache test
 * User: moyo
 * Date: 23/12/2016
 * Time: 10:53 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Cache;

class CacheTest extends \PHPUnit_Framework_TestCase
{
    public function testHostAndClear()
    {
        $key = 'cache-key';
        $ttl = 1;
        $exeCounts = 0;
        $callback = function () use (&$exeCounts) {
            $exeCounts ++;
            return 'hello-'.$exeCounts;
        };

        $result1 = Cache::getInstance()->host($key, $callback, $ttl);
        $this->assertEquals('hello-1', $result1);

        $result2 = Cache::getInstance()->host($key, $callback, $ttl);
        $this->assertEquals('hello-1', $result2);
    }
}