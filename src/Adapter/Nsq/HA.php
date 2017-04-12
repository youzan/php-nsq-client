<?php
/**
 * HA support
 * User: moyo
 * Date: 20/12/2016
 * Time: 5:46 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Exception\NonCatchableException;
use Kdt\Iron\Queue\Foundation\Traits\EventCallback;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use nsqphp\Exception\FailedOnAllNodesException;
use nsqphp\Exception\FailedOnNotLeaderException;
use nsqphp\Exception\LookupException;
use nsqphp\Exception\TopicNotExistException;

use Exception as SysException;

class HA
{
    use SingleInstance;
    use EventCallback;

    /**
     * event for retrying
     */
    const EVENT_RETRYING = 'srv_e_retry';

    /**
     * @param callable $processor
     * @param $identify
     * @param $retryMax
     * @param $retryDelayMS
     * @return mixed
     * @throws TopicNotExistException
     * @throws NonCatchableException
     */
    public function pubRetrying(callable $processor, $identify = 'nsq-publish', $retryMax = 3, $retryDelayMS = 10)
    {
        try
        {
            $result = call_user_func($processor);
        }
        catch (SysException $e)
        {
            if ($e instanceof TopicNotExistException || $e instanceof NonCatchableException)
            {
                // throw it
                throw $e;
            }
            else if ($e instanceof LookupException || $e instanceof FailedOnNotLeaderException || $e instanceof FailedOnAllNodesException)
            {
                // retry it
                if ($retryMax > 0)
                {
                    // logging
                    InstanceMgr::getLoggerInstance()->warn('[HA-Guard] Publish retrying('.$retryMax.') : ['.$identify.'] ~ '.$e->getMessage());
                    // event trigger
                    $this->triggerEvent(self::EVENT_RETRYING, $e);
                    // make delay
                    $retryDelayMS && usleep($retryDelayMS * 1000);
                    // retrying
                    return $this->pubRetrying($processor, $identify, $retryMax - 1, $retryDelayMS);
                }
            }
            $result = ['success' => 0, 'errors' => [get_class($e).' : '.$e->getMessage()]];
        }

        return $result;
    }

    /**
     * @param callable $processor
     * @param string $identify
     * @param int $keepSeconds
     * @param int $retryMax
     * @param int $retryDelay
     * @return mixed
     * @throws TopicNotExistException
     * @throws NonCatchableException
     */
    public function subRetrying(callable $processor, $identify = 'nsq-subscribe', $keepSeconds = 1800, $retryMax = 3, $retryDelay = 5)
    {
        $beginAtTimestamp = time();

        try
        {
            return call_user_func_array($processor, [$keepSeconds]);
        }
        catch (SysException $e)
        {
            if ($e instanceof TopicNotExistException || $e instanceof NonCatchableException)
            {
                // throw it
                throw $e;
            }
            else
            {
                // make retry
                if ($retryMax > 0)
                {
                    // logging
                    InstanceMgr::getLoggerInstance()->warn('[HA-Guard] Subscribe retrying('.$retryMax.') : ['.$identify.'] ~ '.$e->getMessage());
                    // move keep seconds
                    $surplusSeconds = $keepSeconds - (time() - $beginAtTimestamp);
                    // least keep 10s for next sub
                    if ($surplusSeconds >= 10)
                    {
                        // event trigger
                        $this->triggerEvent(self::EVENT_RETRYING, $e);
                        // make delay
                        $retryDelay && sleep($retryDelay);
                        // retrying
                        return $this->subRetrying($processor, $identify, $surplusSeconds, $retryMax - 1, $retryDelay);
                    }
                    else
                    {
                        // timeout ... abandoning
                        return $e->getMessage();
                    }
                }
                else
                {
                    // last retry ... abandoning
                    return $e->getMessage();
                }
            }
        }
    }
}