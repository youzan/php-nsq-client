<?php
/**
 * Queue pool
 * User: moyo
 * Date: 5/7/15
 * Time: 3:50 PM
 */

namespace Kdt\Iron\Queue;

use Kdt\Iron\Queue\Adapter\Nsq\Client;
use Kdt\Iron\Queue\Adapter\Nsq\ServiceChain;
use Kdt\Iron\Queue\Interfaces\MessageInterface;
use Kdt\Iron\Tracing\Sample\Scene\MQ;

class Queue
{
    /**
     * @var int
     */
    private static $maxKeepSeconds = 900;

    /**
     * @var float
     */
    private static $ksRandPercent = 0.25;

    /**
     * @var string
     */
    private static $lastPushError = '';

    /**
     * queue msg publish
     * @param $topic
     * @param Message|Message[] $message
     * @param $options
     * @return bool
     */
    public static function push($topic, $message, array $options = [])
    {
        // options
        $options['max_retry'] = isset($options['max_retry']) ? $options['max_retry'] : 3;
        $options['retry_delay_ms'] = isset($options['retry_delay_ms']) ? $options['retry_delay_ms'] : 10;
        // push
        $TID = MQ::actionBegin($topic, 'publish');
        $result = self::nsq()->push($topic, $message, $options);
        MQ::actionFinish($TID);
        if ($result['error_code'])
        {
            self::$lastPushError = $result['error_message'];
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * queue msg publish (bulk)
     * @param $topic
     * @param Message[] $messages
     * @param $options
     * @return bool
     */
    public static function bulkPush($topic, array $messages, array $options = [])
    {
        // options
        $options['max_retry'] = isset($options['max_retry']) ? $options['max_retry'] : 3;
        $options['retry_delay_ms'] = isset($options['retry_delay_ms']) ? $options['retry_delay_ms'] : 10;
        // push
        $result = self::nsq()->bulk($topic, $messages, $options);
        if ($result['error_code'])
        {
            self::$lastPushError = $result['error_message'];
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * queue subscribe
     * @param $topic
     * @param callable $callback
     * @param $options
     * @return string
     */
    public static function pop($topic, callable $callback, array $options = [])
    {
        // options
        $options['auto_delete'] = isset($options['auto_delete']) ? $options['auto_delete'] : false;
        $options['keep_seconds'] = self::filterKeepSeconds(isset($options['keep_seconds']) ? $options['keep_seconds'] : self::$maxKeepSeconds);
        $options['max_retry'] = isset($options['max_retry']) ? $options['max_retry'] : 3;
        $options['retry_delay'] = isset($options['retry_delay']) ? $options['retry_delay'] : 5;
        $options['sub_ordered'] = isset($options['sub_ordered']) ? $options['sub_ordered'] : false;
        $options['sub_partition'] = isset($options['sub_partition']) ? $options['sub_partition'] : null;
        $options['msg_timeout'] = isset($options['msg_timeout']) ? intval($options['msg_timeout']) : null;

        $serviceChainName = ServiceChain::get(true);
        if ($serviceChainName !== null) {
            $options['tag'] = strval($serviceChainName);
        } else {
            $options['tag'] = isset($options['tag']) ? trim($options['tag']) : null;
        }
        
        // pop
        return self::nsq()->pop
        (
            $topic,
            function (MessageInterface $msg) use ($callback)
            {
                $zanTest = $msg->getExtends('zan_test');
                if ($zanTest && $ext['zan_test'] !== 'false') {
                    $serviceChain = ServiceChain::getAll();
                    $serviceChain['zan_test'] = true;
                    ServiceChain::setAll($serviceChain);
                }
                call_user_func_array($callback, [$msg]);
            },
            $options
        );
    }

    /**
     * exiting pop loop
     */
    public static function exitPop()
    {
        self::nsq()->stop();
    }

    /**
     * queue msg done
     * @param $messageId
     * @return bool
     */
    public static function delete($messageId)
    {
        return self::nsq()->delete($messageId);
    }

    /**
     * queue msg delay
     * @param $seconds
     */
    public static function later($seconds)
    {
        self::nsq()->later($seconds);
    }

    /**
     * queue msg retry
     */
    public static function retry()
    {
        self::nsq()->retry();
    }

    /**
     * get last push error
     * @return string
     */
    public static function lastPushError()
    {
        return self::$lastPushError;
    }

    /**
     * close all connections
     */
    public static function close()
    {
        self::nsq()->close();
    }

    /**
     * send a nop command
     */
    public static function ping()
    {
        self::nsq()->ping();
    }

    /**
     * @return Client
     */
    private static function nsq()
    {
        static $nsqInstance = null;
        if (is_null($nsqInstance))
        {
            $nsqInstance = new Client();
        }
        return $nsqInstance;
    }

    /**
     * @param $custom
     * @return int
     */
    private static function filterKeepSeconds($custom)
    {
        $custom = intval($custom);
        // limit
        $custom = $custom > self::$maxKeepSeconds ? self::$maxKeepSeconds : $custom;
        // random
        $random = mt_rand(0, intval($custom * self::$ksRandPercent));
        // merge
        return $custom + $random;
    }
}
