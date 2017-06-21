<?php

namespace nsqphp\Connection;

interface ConnectionInterface
{
    /**
     * Get proxy bridge
     * @return Proxy
     */
    public function getProxy();

    /**
     * Set proxy bridge
     * @param $proxy
     */
    public function setProxy(Proxy $proxy);

    /**
     * Get connection type
     * @return string // tcp or http
     */
    public function getConnType();

    /**
     * Get partition id
     * @return string
     */
    public function getPartitionID();

    /**
     * Check is new Yz-Cluster
     * @return bool
     */
    public function isYzCluster();

    /**
     * Wait for readable
     * 
     * Waits for the socket to become readable (eg: have some data waiting)
     * 
     * @return boolean
     */
    public function isReadable();

    /**
     * Read from the socket exactly $len bytes
     *
     * @param integer $len How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($len);
    
    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     */
    public function write($buf);
    
    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket();

    /**
     * Reconnect and replace the socket resource.
     *
     * @return Resource The socket, after reconnecting
     */
    public function reconnect();

    /**
     * Check socket is connected yes
     *
     * @return bool
     */
    public function connected();

    /**
     * Close connection
     * @return bool
     */
    public function close();

    /**
     * Http posting
     * @param $api
     * @param $body
     * @return string
     */
    public function post($api, $body);

    /**
     * Set subscribe ordered
     */
    public function setOrderedSub();

    /**
     * Check subscribe is ordered or not
     * @return bool
     */
    public function isOrderedSub();

    /**
     * desired tag
     * @return string|null
     */
    public function getDesiredTag();

    /**
     * has extend data
     * @return bool
     */
    public function getHasExtendData();
}
