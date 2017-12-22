<?php
/**
 * Message filter
 * User: moyo
 * Date: 21/12/2016
 * Time: 6:03 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Feature\MsgSharding;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Kdt\Iron\Queue\Message as QMessage;
use nsqphp\Message\Message as NSQMessage;

class MsgFilter
{
    use SingleInstance;


    /**
     * @var MsgSharding
     */
    private $msgSharding = null;

    /**
     * MsgFilter constructor.
     */
    public function __construct()
    {
        $this->msgSharding = MsgSharding::getInstance();
    }

    /**
     * @param $topic
     * @param $bizMsg
     * @return NSQMessage
     */
    public function getMsgObject($topic, $bizMsg)
    {
        return $this->getTargetMsg($topic, $bizMsg, false);
    }

    /**
     * @param $topic
     * @param $bizMsgList
     * @return NSQMessage[]
     */
    public function getMsgObjectBag($topic, $bizMsgList)
    {
        $bag = [];
        foreach ($bizMsgList as $message)
        {
            $bag[] = $this->getTargetMsg($topic, $message, true);
        }
        return $bag;
    }

    /**
     * @param $topic
     * @param $bizMsg
     * @param $inBag
     * @return NSQMessage
     */
    private function getTargetMsg($topic, $bizMsg, $inBag)
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
        
        $target->setTag($origin->getTag());
        $ext = $target->getExtends();
        if ($zanTest !== false) {
            $ext['zan_test'] = true;
        }
        $oriExt = $origin->getAllExtends();
        if (!empty($oriExt)) {
            foreach ($oriExt as $k => $v) {
                $ext[$k] = $v;
            }
        }
        $target->setExtends($ext);

        $this->msgSharding->process($topic, $origin, $target, $inBag);

        // finish

        return $target;
    }
}
