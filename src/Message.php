<?php
/**
 * Queue message
 * User: moyo
 * Date: 5/7/15
 * Time: 3:45 PM
 */

namespace Kdt\Iron\Queue;

use Kdt\Iron\Queue\Exception\InvalidParameterException;
use Kdt\Iron\Queue\Interfaces\MessageInterface;

class Message implements MessageInterface
{
    /**
     * msg id
     * @var null
     */
    private $id = null;

    /**
     * msg create timestamp
     * @var int
     */
    private $timestamp = 0;

    /**
     * msg attempts count
     * @var int
     */
    private $attempts = 0;

    /**
     * msg payload
     * @var string
     */
    private $payload = null;

    /**
     * msg traceID
     * @var int
     */
    private $traceID = null;

    /**
     * msg shardingProof
     * @var int
     */
    private $shardingProof = null;

    /**
     * msg tag
     * @var string
     */
    private $tag = null;

    /**
     * @param $mix1
     * @param $timestamp
     * @param $attempts
     * @param $payload
     *
     * @throws InvalidParameterException
     */
    public function __construct($mix1, $timestamp = null, $attempts = null, $payload = null)
    {
        $argsCount = func_num_args();

        if ($argsCount == 1)
        {
            // from producer
            // new Message('msg-data')
            $this->payload = json_encode($mix1);
        }
        else
        {
            // from consumer
            // new Message('id', 'timestamp', 'attempts', 'payload')
            $this->id = (string)$mix1;
            $this->timestamp = (int)substr($timestamp, 0, 10);
            $this->attempts = (int)$attempts;
            $this->payload = json_decode($payload, true);
        }
    }

    /**
     * msg id
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * msg create timestamp
     * @return int
     */
    public function getTimestamp()
    {
        return $this->timestamp;
    }

    /**
     * msg attempts count
     * @return int
     */
    public function getAttempts()
    {
        return $this->attempts;
    }

    /**
     * msg payload
     * @return mixed
     */
    public function getPayload()
    {
        return $this->payload;
    }

    /**
     * msg traceID
     * @return int
     */
    public function getTraceID()
    {
        return $this->traceID;
    }

    /**
     * msg shardingProof
     * @return int
     */
    public function getShardingProof()
    {
        return $this->shardingProof;
    }

    /**
     * msg tag
     * @return string
     */
    public function getTag()
    {
        return $this->tag;
    }

    /**
     * @param $id
     * @return static
     * @throws InvalidParameterException
     */
    public function setTraceID($id)
    {
        if (is_numeric($id))
        {
            $this->traceID = $id;
        }
        else
        {
            throw new InvalidParameterException('NSQ traceID must be an integer', 9985);
        }
        return $this;
    }

    /**
     * @param $sample
     * @return static
     * @throws InvalidParameterException
     */
    public function setShardingProof($sample)
    {
        if (is_numeric($sample))
        {
            $this->shardingProof = $sample;
        }
        else
        {
            throw new InvalidParameterException('NSQ shardingProof must be an integer', 9986);
        }
        return $this;
    }

    /**
     * @param $tag
     * @return static
     */
    public function setTag($tag)
    {
        $this->tag = $tag;
        return $this;
    }
}
