<?php

namespace nsqphp\Message;

interface MessageInterface
{
    /**
     * Get message payload
     * 
     * @return string
     */
    public function getPayload();
    
    /**
     * Get message ID
     * 
     * @return string|NULL
     */
    public function getId();

    /**
     * Get message ID as raw type
     *
     * @return string|NULL
     */
    public function getRawId();

    /**
     * Get message trace ID
     *
     * @return integer|NULL
     */
    public function getTraceId();
    
    /**
     * Get attempts
     * 
     * @return integer|NULL
     */
    public function getAttempts();
    
    /**
     * Get timestamp
     * 
     * @return float|NULL
     */
    public function getTimestamp();

    /**
     * Get limitedNode info
     * Message will only pub to this node
     *
     * @return array
     */
    public function getLimitedNode();
    
    /**
     * Message tag
     * @return string|NULL
     */
    public function getTag();
}