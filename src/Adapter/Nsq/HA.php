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
     * @param $topic
     * @param callable $processor
     * @param $retryMax
     * @return mixed
     * @throws TopicNotExistException
     * @throws NonCatchableException
     */
    public function pubRetrying($topic, callable $processor, $retryMax = 3)
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
                    $this->triggerEvent(self::EVENT_RETRYING, $e);
                    return $this->pubRetrying($topic, $processor, $retryMax - 1);
                }
            }
            $result = ['success' => 0, 'errors' => [get_class($e).' : '.$e->getMessage()]];
        }

        return $result;
    }

    /**
     * @param callable $processor
     * @param int $keepSeconds
     * @param int $retryMax
     * @param int $retryDelay
     * @param string $identify
     * @return mixed
     */
    public function subRetrying(callable $processor, $keepSeconds = 1800, $retryMax = 3, $retryDelay = 5, $identify = 'nsq-subscribe')
    {
        $beginAtTimestamp = time();

        try
        {
            return call_user_func_array($processor, [$keepSeconds]);
        }
        catch (SysException $e)
        {
            // make retry
            if ($retryMax > 0)
            {
                // logging
                InstanceMgr::getLoggerInstance()->warn('[IRON] Subscribe retrying('.$retryMax.') : ['.$identify.'] ~ '.$e->getMessage());
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
                    return $this->subRetrying($processor, $surplusSeconds, $retryMax - 1, $retryDelay, $identify);
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