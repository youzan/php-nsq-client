<?php
/**
 * HA test
 * User: moyo
 * Date: 23/12/2016
 * Time: 4:52 PM
 */

namespace Kdt\Iron\Queue\Tests\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\HA;
use Kdt\Iron\Queue\Exception\InvalidConfigException;
use Kdt\Iron\Queue\Exception\InvalidParameterException;
use Kdt\Iron\Queue\Exception\MissingRoutesException;
use Kdt\Iron\Queue\Exception\ServiceInitializationException;
use Kdt\Iron\Queue\Exception\UnknownSubInstanceException;
use nsqphp\Exception\FailedOnAllNodesException;
use nsqphp\Exception\FailedOnNotLeaderException;
use nsqphp\Exception\LookupException;
use nsqphp\Exception\TopicNotExistException;
use RuntimeException;
use Exception;

class HATest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var array
     */
    private $nonCatchExceptions = [
        TopicNotExistException::class,
        InvalidConfigException::class,
        InvalidParameterException::class,
        MissingRoutesException::class,
        ServiceInitializationException::class,
        UnknownSubInstanceException::class
    ];

    /**
     * @var array
     */
    private $pubRetryExceptions = [
        LookupException::class,
        FailedOnNotLeaderException::class,
        FailedOnAllNodesException::class
    ];

    /**
     * @var array
     */
    private $subRetryExceptions = [
        LookupException::class,
        RuntimeException::class
    ];

    /**
     * @var int
     */
    private $leastKeepSeconds = 10;

    /**
     * @var int
     */
    private $mostIgnoreSeconds = 9;

    public function testPubRetryingWithOK()
    {
        $result = HA::getInstance()->pubRetrying(function () {
            return 'hello';
        });

        $this->assertEquals('hello', $result);
    }

    public function testPubRetryingWithFailed()
    {
        $executed = 0;
        $result = HA::getInstance()->pubRetrying(function () use (&$executed) {
            $executed ++;
            throw new \Exception('e-result');
        });
        $expect = [
            'success' => 0,
            'errors' => [
                'Exception : e-result'
            ]
        ];

        $this->assertEquals(1, $executed);
        $this->assertArraySubset($result, $expect);
    }

    public function testPubRetryingRetried()
    {
        $testException = $this->pubRetryExceptions[rand(0, count($this->pubRetryExceptions) - 1)];

        $executed = 0;
        $result = HA::getInstance()->pubRetrying(function () use ($testException, &$executed) {
            $executed ++;
            throw new $testException;
        }, 'some-identify', 3, 0);
        $expect = [
            'success' => 0,
            'errors' => [
                $testException.' : '
            ]
        ];

        // first time + retry 3 times
        $this->assertEquals(4, $executed);
        $this->assertArraySubset($result, $expect);
    }

    public function testSubRetryingWithOK()
    {
        $mockKeepSeconds = 123;
        $result = HA::getInstance()->subRetrying(function ($maxKeepSeconds) {
            return $maxKeepSeconds;
        }, 'some-identify', $mockKeepSeconds);
        $this->assertEquals($result, $mockKeepSeconds);
    }

    public function testSubRetryingTimeout()
    {
        $executed = 0;
        $result = HA::getInstance()->subRetrying(function () use (&$executed) {
            $executed ++;
            throw new Exception('timeout-no-retry');
        }, 'some-identify', $this->mostIgnoreSeconds);

        $this->assertEquals(1, $executed);
        $this->assertEquals('timeout-no-retry', $result);
    }

    public function testSubRetryingRetried()
    {
        $testException = $this->subRetryExceptions[rand(0, count($this->subRetryExceptions) - 1)];

        $executed = 0;
        $lastMsg = uniqid();
        $result = HA::getInstance()->subRetrying(function () use ($testException, &$executed, $lastMsg) {
            $executed ++;
            throw new $testException($lastMsg);
        }, 'some-identify', $this->leastKeepSeconds, 3, 0);

        // first time + retry 3 times
        $this->assertEquals(4, $executed);
        $this->assertEquals($lastMsg, $result);
    }

    public function testMixRetryingNonCatch()
    {
        $methods = [
            'pubRetrying', 'subRetrying'
        ];
        foreach ($methods as $method)
        {
            $executed = 0;
            foreach ($this->nonCatchExceptions as $exceptionClass)
            {
                $exceptionGot = null;
                try
                {
                    call_user_func_array(
                        [HA::getInstance(), $method],
                        [function () use ($exceptionClass) {
                            throw new $exceptionClass;
                        }]
                    );
                    $executed ++;
                }
                catch (Exception $e)
                {
                    $exceptionGot = $e;
                }
                $this->assertInstanceOf($exceptionClass, $exceptionGot);
            }
            $this->assertEquals(0, $executed);
        }
    }
}