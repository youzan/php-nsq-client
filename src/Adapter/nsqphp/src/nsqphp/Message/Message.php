<?php

namespace nsqphp\Message;

class Message implements MessageInterface
{
    /**
     * Construct from frame
     *
     * @param array $frame
     * @return Message
     */
    public static function fromFrame(array $frame)
    {
        $ret = new Message(
                $frame['payload'],
                $frame['trace_id'],
                $frame['dsp_id'],
                $frame['raw_id'],
                $frame['attempts'],
                $frame['ts']
                );
        if (isset($frame['tag']))
        {
            $ret->setTag($frame['tag']);
        }
        if (isset($frame['extends']))
        {
            $ret->setExtends($frame['extends']);
        }
        return $ret;
    }
    
    private $extends = [];
   
    /**
     * Message payload - string
     * 
     * @var string
     */
    private $data = '';

    /**
     * Message trace ID; HA Cluster featured
     *
     * @var null
     */
    private $trace_id = NULL;
    
    /**
     * Message ID; if relevant; for display
     * 
     * @var string|NULL
     */
    private $dsp_id = NULL;

    /**
     * Message ID; for protocol
     *
     * @var string|NULL
     */
    private $raw_id = NULL;
    
    /**
     * How many attempts have been made; if relevant
     * 
     * @var integer|NULL
     */
    private $attempts = NULL;
    
    /**
     * Timestamp - UNIX timestamp in seconds (incl. fractions); if relevant
     * 
     * @var float|NULL
     */
    private $ts = NULL;

    /**
     * LimitedNode - JUST for PUB. Message will limited to this node
     *
     * @var array
     */
    private $limitedNode = NULL;

    /**
     * Constructor
     * 
     * @param string $data
     * @param integer|NULL $trace_id The message trace ID as integer
     * @param string|NULL $dsp_id The message ID in hex (as ASCII)
     * @param string|NULL $raw_id The message ID in raw (maybe binary)
     * @param integer|NULL $attempts How many attempts have been made on msg so far
     * @param float|NULL $ts Timestamp (nanosecond precision, as number of seconds)
     */
    public function __construct($data, $trace_id = NULL, $dsp_id = NULL, $raw_id = NULL, $attempts = NULL, $ts = NULL)
    {
        $this->data = $data;
        $this->trace_id = $trace_id;
        $this->dsp_id = $dsp_id;
        $this->raw_id = $raw_id;
        $this->attempts = $attempts;
        $this->ts = $ts;
    }
    
    /**
     * Get message payload
     * 
     * @return string
     */
    public function getPayload()
    {
        return $this->data;
    }

    /**
     * Get message ID
     * 
     * @return string|NULL
     */
    public function getId()
    {
        return $this->dsp_id;
    }

    /**
     * Get message ID as raw type
     *
     * @return string|NULL
     */
    public function getRawId()
    {
        return $this->raw_id;
    }

    /**
     * Get message trace ID
     *
     * @return integer|NULL
     */
    public function getTraceId()
    {
        $ret = $this->trace_id;
        if (!empty($ret)) {
            return $ret;
        }
        if (isset($this->extends['##trace_id']))
        {
            return $this->extends['##trace_id'];
        }
    }

    /**
     * Get attempts
     * 
     * @return integer|NULL
     */
    public function getAttempts()
    {
        return $this->attempts;
    }
    
    /**
     * Get timestamp
     * 
     * @return float|NULL
     */
    public function getTimestamp()
    {
        return $this->ts;
    }

    /**
     * @return array
     */
    public function getLimitedNode()
    {
        return $this->limitedNode;
    }

    /**
     * @param $id
     */
    public function setTraceId($id)
    {
        $this->extends['##trace_id'] = $id;
        $this->trace_id = $id;
    }

    /**
     * @param $node
     */
    public function setLimitedNode($node)
    {
        $this->limitedNode = $node;
    }
    
    /**
     * @param $tag
     */
    public function setTag($tag) 
    {
        $this->extends['##client_dispatch_tag'] = $tag;
    }
    
    /**
     * @return string|NULL
     */
    public function getTag()
    {
        if (isset($this->extends['##client_dispatch_tag'])) {
            return $this->extends['##client_dispatch_tag'];
        }
        return null;
    }

    public function setExtends($extends)
    {
        $this->extends = $extends;
    }

    public function getExtends()
    {
        return $this->extends;
    }
}
