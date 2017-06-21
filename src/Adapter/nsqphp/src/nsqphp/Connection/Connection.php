<?php

namespace nsqphp\Connection;

use nsqphp\Exception\ConnectionException;
use nsqphp\Exception\PublishException;
use nsqphp\Exception\SocketException;

/**
 * Represents a single connection to a single NSQD server
 */
class Connection implements ConnectionInterface
{
    /**
     * Hostname
     * 
     * @var string
     */
    private $hostname;
    
    /**
     * Port number (tcp)
     * 
     * @var integer
     */
    private $portTcp;

    /**
     * Port number (http)
     * @var integer
     */
    private $portHttp;

    /**
     * Cluster type
     * @var boolean
     */
    private $isYzCluster;

    /**
     * Cluster partition id
     * @var string
     */
    private $partitionID;

    /**
     * Subscribe in ordered state
     * @var bool
     */
    private $staIsOrderedSub = false;

    private $hasExt = false;

    private $tag = null;

    /**
     * With proxy
     *
     * @var Proxy
     */
    private $proxy;

    /**
     * Connection type (tcp or http)
     * @var string
     */
    private $connectionType;
    
    /**
     * Connection timeout - in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Read/write timeout - in whole seconds
     * 
     * @var integer
     */
    private $readWriteTimeoutSec;
    
    /**
     * Read/write timeout - in whole microseconds
     * 
     * (to be added to the whole seconds above)
     * 
     * @var integer
     */
    private $readWriteTimeoutUsec;

    /**
     * Read wait timeout - in whole seconds
     * 
     * @var integer
     */
    private $readWaitTimeoutSec;

    /**
     * Read wait timeout - in whole microseconds
     * 
     * (to be added to the whole seconds above)
     * 
     * @var integer
     */
    private $readWaitTimeoutUsec;
    
    /**
     * Non-blocking mode?
     * 
     * @var boolean
     */
    private $nonBlocking;
    
    /**
     * Optional on-connect callback
     * 
     * @var callable|NULL
     */
    private $connectCallback;

    /**
     * Socket handle
     * 
     * @var Resource|NULL
     */
    private $socket = NULL;

    /**
     * Timestamp of the last connection
     *
     * @var int
     */
    private $lastConnectionTime;

    /**
     * Time after which the connection will be recycled
     *
     * @var int
     */
    private $connectionRecycling;

    /**
     * Constructor
     *
     * @param string $hostname Default localhost
     * @param integer $portTcp Default 4150
     * @param integer $portHttp Default 4151
     * @param boolean $isCluster cluster type
     * @param string $partitionID cluster partition
     * @param string $connectionType tcp or http
     * @param float $connectionTimeout In seconds (no need to be whole numbers)
     * @param float $readWriteTimeout Socket timeout during active read/write
     *      In seconds (no need to be whole numbers)
     * @param float $readWaitTimeout How long we'll wait for data to become
     *      available before giving up (eg; duirng SUB loop)
     *      In seconds (no need to be whole numbers)
     * @param boolean $nonBlocking Put socket in non-blocking mode
     * @param callable|NULL $connectCallback Optional on-connect callback (will
     *      be called whenever we establish a connection)
     * @param int|NULL $connectionRecycling In seconds, time after which the
     *      connection will be recycled
     */
    public function __construct(
            $hostname = 'localhost',
            $portTcp = NULL,
            $portHttp = NULL,
            $isCluster = FALSE,
            $partitionID = NULL,
            $connectionType = 'tcp',
            $connectionTimeout = 3.0,
            $readWriteTimeout = 3.0,
            $readWaitTimeout = 15.0,
            $nonBlocking = FALSE,
            $connectCallback = NULL,
            $connectionRecycling = NULL
            ) {
        $this->hostname = $hostname;
        $this->portTcp = $portTcp ? $portTcp : 4150;
        $this->portHttp = $portHttp ? $portHttp : 4151;
        $this->isYzCluster = $isCluster;
        $this->partitionID = $partitionID;
        $this->connectionType = $connectionType;
        $this->connectionTimeout = $connectionTimeout;
        $this->readWriteTimeoutSec = floor($readWriteTimeout);
        $this->readWriteTimeoutUsec = ($readWriteTimeout - $this->readWriteTimeoutSec) * 1000000;
        $this->readWaitTimeoutSec = floor($readWaitTimeout);
        $this->readWaitTimeoutUsec = ($readWaitTimeout - $this->readWaitTimeoutSec) * 1000000;
        $this->nonBlocking = (bool)$nonBlocking;
        $this->connectCallback = $connectCallback;
        $this->connectionRecycling = $connectionRecycling;
    }

    /**
     * @return Proxy
     */
    public function getProxy()
    {
        return $this->proxy;
    }

    /**
     * @param Proxy $proxy
     */
    public function setProxy(Proxy $proxy)
    {
        $this->proxy = $proxy;
    }

    /**
     * @return string
     */
    public function getConnType()
    {
        return $this->connectionType;
    }

    /**
     * @return bool
     */
    public function isYzCluster()
    {
        return $this->isYzCluster;
    }

    /**
     * @return string
     */
    public function getPartitionID()
    {
        return $this->partitionID;
    }

    /**
     * Set subscribe ordered
     */
    public function setOrderedSub()
    {
        $this->staIsOrderedSub = true;
    }

    /**
     * Check subscribe is ordered or not
     * @return bool
     */
    public function isOrderedSub()
    {
        return $this->staIsOrderedSub;
    }

    /**
     * desired tag
     * @return string|null
     */
    public function getDesiredTag()
    {
        return $this->tag;
    }

    /**
     * set desired tag
     * @param string|null $tag
     */
    public function setDesiredTag($tag)
    {
        $this->tag = $tag;
    }

    /**
     * @return bool
     */
    public function getHasExtendData()
    {
        return $this->hasExt;
    }

    /**
     * 
     * @param bool $hasExt
     */
    public function setHasExtendData($hasExt)
    {
        $this->hasExt = $hasExt;
    }
     

    /**
     * Wait for readable
     * 
     * Waits for the socket to become readable (eg: have some data waiting)
     * 
     * @return boolean
     */
    public function isReadable()
    {
        $null = NULL;
        $read = [$this->getSocket()];
        $readable = stream_select($read, $null, $null, $this->readWaitTimeoutSec, $this->readWaitTimeoutUsec);
        return $readable ? TRUE : FALSE;
    }
    
    /**
     * Read from the socket exactly $len bytes
     *
     * @param integer $desiredLen How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($desiredLen)
    {
        $null = NULL;
        $socket = $this->getSocket();
        $read = [$socket];
        $surplusLen = $desiredLen;
        $data = '';

        while (strlen($data) < $desiredLen) {
            $readable = @stream_select($read, $null, $null, $this->readWriteTimeoutSec, $this->readWriteTimeoutUsec);
            if ($readable > 0) {
                $buffer = @stream_socket_recvfrom($socket, $surplusLen);
                if ($buffer === FALSE) {
                    throw new SocketException("Could not read {$surplusLen} bytes from {$this->hostname}:{$this->portTcp}");
                } else if ($buffer == '') {
                    throw new SocketException("Read 0 bytes from {$this->hostname}:{$this->portTcp}");
                }
            } else if ($readable === 0) {
                throw new SocketException("Timed out reading {$surplusLen} bytes from {$this->hostname}:{$this->portTcp} after {$this->readWriteTimeoutSec} seconds and {$this->readWriteTimeoutUsec} microseconds");
            } else {
                throw new SocketException("Could not read {$surplusLen} bytes from {$this->hostname}:{$this->portTcp}");
            }
            $data .= $buffer;
            $surplusLen -= strlen($buffer);
        }
        return $data;
    }

    /**
     * Reconnect and return the socket
     *
     * @return Resource the socket
     */
    public function reconnect()
    {
        $this->close();
        return $this->getSocket();
    }

    /**
     * Check socket is connected yes
     *
     * @return bool
     */
    public function connected()
    {
        return ! is_null($this->socket);
    }

    /**
     * Close connection
     * @return bool
     */
    public function close()
    {
        $closed = @fclose($this->socket);
        $this->socket = NULL;
        return $closed;
    }

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     */
    public function write($buf)
    {
        $null = NULL;
        $socket = $this->getSocket();
        $write = [$socket];

        // keep writing until all the data has been written
        while (strlen($buf) > 0) {
            // wait for stream to become available for writing
            $writable = @stream_select($null, $write, $null, $this->readWriteTimeoutSec, $this->readWriteTimeoutUsec);
            if ($writable > 0) {
                // write buffer to stream
                $written = @stream_socket_sendto($socket, $buf);
                if ($written === -1 || $written === FALSE) {
                    throw new SocketException("Could not write " . strlen($buf) . " bytes to {$this->hostname}:{$this->portTcp}");
                }
                // determine how much of the buffer is left to write
                $buf = substr($buf, $written);
            } else if ($writable === 0) {
                throw new SocketException("Timed out writing " . strlen($buf) . " bytes to {$this->hostname}:{$this->portTcp} after {$this->readWriteTimeoutSec} seconds and {$this->readWriteTimeoutUsec} microseconds");
            } else {
                throw new SocketException("Could not write " . strlen($buf) . " bytes to {$this->hostname}:{$this->portTcp}");
            }
        }
    }
    
    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket()
    {
        // connection recycling
        if ($this->connectionRecycling &&
            $this->socket &&
            time() - $this->lastConnectionTime >= $this->connectionRecycling) {
            $this->close();
        }
        if ($this->socket === NULL) {
            $connectionTimeout = $this->connectionTimeout;
            $this->socket = $this->proxy->initSocket($this->hostname, $this->portTcp, function ($host, $port) use(&$errNo, &$errStr, $connectionTimeout) {
                return fsockopen($host, $port, $errNo, $errStr, $connectionTimeout);
            });
            if ($this->socket === FALSE) {
                throw new ConnectionException(
                        "Could not connect to {$this->hostname}:{$this->portTcp} ({$errStr} [{$errNo}])"
                        );
            }
            $this->lastConnectionTime = time();
            if ($this->nonBlocking) {
                stream_set_blocking($this->socket, 0);
            }
            
            // on-connection callback
            if ($this->connectCallback !== NULL) {
                call_user_func($this->connectCallback, $this);
            }
        }
        return $this->socket;
    }

    /**
     * @param $api
     * @param $body
     * @return string
     */
    public function post($api, $body)
    {
        $connection = $this;
        return $this->proxy->httpPost($this->hostname, $this->portHttp, function ($host, $port) use($api, $body, $connection) {

            $url = 'http://'.$host.':'.$port.$api;

            $timeout = [
                CURLOPT_CONNECTTIMEOUT => $this->connectionTimeout,
                CURLOPT_TIMEOUT        => $this->readWriteTimeoutSec,
            ];

            list($error, $response) = HTTP::post($url, $body, $timeout);

            if ($error)
            {
                list($errorNo, $errorMsg) = $error;
                throw new PublishException('nsqd - '.$url.' : '.$errorMsg, $errorNo);
            }
            else
            {
                return $response;
            }
        });
    }

    /**
     * To string (for debug logging)
     * 
     * @return string
     */
    public function __toString()
    {
        $partitionID = is_numeric($this->partitionID) ? $this->partitionID : 'N';
        return "{$this->hostname}:{$this->portTcp}/{$this->portHttp}/-{$partitionID}";
    }
}
