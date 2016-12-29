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
    public function testLookupsResult()
    {
        $testData = [
            'dsn_topic_dcc_default' => [
                'sub' => ['global-dcc-default', ['http://127.0.0.3:123']],
                'pub' => ['global-dcc-default', ['http://127.0.0.3:123']],
            ],
            'dsn_topic_dcc_before' => [
                'sub' => ['global-dcc-before', ['http://127.0.0.3:1']],
                'pub' => ['global-dcc-before', ['http://127.0.0.3:1']],
            ],
            'dsn_topic_dcc_moving_gray_miss' => [
                'sub' => ['global-dcc-moving', ['http://127.0.0.3:1', 'http://127.0.0.3:2']],
                'pub' => ['global-dcc-moving', ['http://127.0.0.3:1']],
            ],
            'dsn_topic_dcc_moving_gray_hit' => [
                'sub' => ['global-dcc-moving', ['http://127.0.0.3:1', 'http://127.0.0.3:2']],
                'pub' => ['global-dcc-moving', ['http://127.0.0.3:2']],
            ],
            'dsn_topic_dcc_moving_non' => [
                'sub' => ['global-dcc-moving', ['http://127.0.0.3:1', 'http://127.0.0.3:2']],
                'pub' => ['global-dcc-moving', ['http://127.0.0.3:1']],
            ],
            'dsn_topic_dcc_finish' => [
                'sub' => ['global-dcc-finish', ['http://127.0.0.3:2']],
                'pub' => ['global-dcc-finish', ['http://127.0.0.3:2']],
            ],
            'binlog_dcc_named_special' => [
                'sub' => ['global-dcc-binlog', ['http://127.0.0.3:2']],
                'pub' => ['global-dcc-binlog', ['http://127.0.0.3:2']],
            ]
        ];

        foreach ($testData as $topic => $rw)
        {
            foreach ($rw as $scene => $expect)
            {
                list($expCluster, $expLookups) = $expect;

                $pipe = $scene == 'sub' ? 'r' : 'w';

                $gotLookups = Router::getInstance()->fetchGlobalLookups($topic, $scene);

                $gotResult = [$pipe => $gotLookups[$pipe]];
                $expResult = [$pipe => [$expCluster => $expLookups]];

                $this->assertArraySubset($gotResult, $expResult, TRUE, 'topic='.$topic.',scene='.$scene);
            }
        }
    }
}