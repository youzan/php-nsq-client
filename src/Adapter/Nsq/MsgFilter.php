<?php
/**
 * Message filter
 * User: moyo
 * Date: 21/12/2016
 * Time: 6:03 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Feature\MsgSharding;
use Kdt\Iron\Queue\Adapter\Nsq\Feature\MsgTracing;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Kdt\Iron\Queue\Message as QMessage;
use nsqphp\Message\Message as NSQMessage;

class MsgFilter
{
    use SingleInstance;

    /**
     * @var MsgTracing
     */
    private $msgTracing = null;

    /**
     * @var MsgSharding
     */
    private $msgSharding = null;

    /**
     * MsgFilter constructor.
     */
    public function __construct()
    {
        $this->msgTracing = MsgTracing::getInstance();
        $this->msgSharding = MsgSharding::getInstance();
    }

    /**
     * @param $topic
     * @param $bizMsg
     * @return NSQMessage
     */
    public function getMsgObject($topic, $bizMsg)
    {
        if (is_object($bizMsg) && $bizMsg instanceof QMessage)
        {
            $origin = $bizMsg;
        }
        else
        {
            $origin = new QMessage($bizMsg);
        }

        $target = new NSQMessage($origin->getPayload());

        // flows

        // s1 - msg tracing
        $this->msgTracing->process($topic, $origin, $target);
        // s2 - msg sharding
        $this->msgSharding->process($topic, $origin, $target);

        // finish

        return $target;
    }
}