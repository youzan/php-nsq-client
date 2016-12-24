<?php
/**
 * Router test for DCC
 * User: moyo
 * Date: 25/12/2016
 * Time: 2:07 AM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Router;

class RouterDCCTest extends \PHPUnit_Framework_TestCase
{
    public function testBeforeState()
    {
        $lookups = Router::getInstance()->fetchGlobalLookups('dsn_topic_dcc_before');
        $expect = [
            'r' => ['global-dcc-before' => ['http://127.0.0.3:1']],
            'w' => ['global-dcc-before' => ['http://127.0.0.3:1']],
        ];

        $this->assertArraySubset($lookups, $expect, TRUE);
    }

    public function testMovingState()
    {

    }

    public function testFinishState()
    {

    }
}