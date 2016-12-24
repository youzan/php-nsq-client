<?php
/**
 * Message tracing
 * User: moyo
 * Date: 21/12/2016
 * Time: 5:50 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq\Feature;

use Kdt\Iron\Config\Live\DCC;
use Kdt\Iron\Queue\Adapter\Nsq\Config;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Kdt\Iron\Queue\Message as QMessage;
use nsqphp\Message\Message as NSQMessage;

use Exception as SysException;

class MsgTracing
{
    use SingleInstance;

    /**
     * @var array
     */
    private $topicEnables = [];

    /**
     * @var bool
     */
    private $realtimeDetect = false;

    /**
     * @var Config
     */
    private $config = null;

    /**
     * MsgTracing constructor.
     */
    public function __construct()
    {
        $this->config = Config::getInstance();
    }

    /**
     * @param $topic
     * @param QMessage $origin
     * @param NSQMessage $target
     * @param bool $inBag
     */
    public function process($topic, QMessage $origin, NSQMessage $target, $inBag)
    {
        // whatever $inBag

        $nsqTraceID = 0;

        $bizTraceID = $origin->getTraceID();
        if ($bizTraceID)
        {
            $reDetect = false;

            if ($this->realtimeDetect)
            {
                // realtime detect will always detect
                $reDetect = true;
            }
            else
            {
                if (isset($this->topicEnables[$topic]))
                {
                    $this->topicEnables[$topic] && $nsqTraceID = $bizTraceID;
                }
                else
                {
                    // not found local enabled stat
                    $reDetect = true;
                }
            }

            if ($reDetect)
            {
                try
                {
                    $dccSwitch = DCC::get(['nsq', 'topic.trace', $this->config->parseTopicName($topic)]);
                }
                catch (SysException $e)
                {
                    $dccSwitch = 0;
                }

                $traceEnabled = (int)$dccSwitch ? true : false;

                $this->topicEnables[$topic] = $traceEnabled;

                if ($traceEnabled)
                {
                    $nsqTraceID = $bizTraceID;
                }
            }
        }

        if ($nsqTraceID)
        {
            $target->setTraceId($nsqTraceID);
        }
    }
}