<?php
/**
 * Message interface
 * User: moyo
 * Date: 5/11/15
 * Time: 3:17 PM
 */

namespace Kdt\Iron\Queue\Interfaces;

interface MessageInterface
{
    /**
     * msg id
     * @return string
     */
    public function getId();

    /**
     * msg create timestamp
     * @return int
     */
    public function getTimestamp();

    /**
     * msg attempts count
     * @return int
     */
    public function getAttempts();

    /**
     * msg payload
     * @return mixed
     */
    public function getPayload();

    /**
     * msg traceID
     * @return int
     */
    public function getTraceID();

    /**
     * msg shardingProof
     * @return int
     */
    public function getShardingProof();

    /**
     * @param $id
     * @return static
     */
    public function setTraceID($id);

    /**
     * @param $sample
     * @return static
     */
    public function setShardingProof($sample);
    
    /**
     * @param $tag
     * @return static
     */
    public function setTag($tag);
    
    /**
     * msg tag
     * @return string
     */
    public function getTag();
    
    public function setExtends($k, $v);
    
    public function getExtends($k);
    
}