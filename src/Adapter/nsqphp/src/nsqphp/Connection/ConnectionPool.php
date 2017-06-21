<?php

namespace nsqphp\Connection;

/**
 * Represents a pool of connections to one or more NSQD servers
 */
class ConnectionPool implements \Iterator, \Countable
{
    /**
     * Connections
     *
     * @var ConnectionInterface[] $connection
     */
    private $connections = array();

    /**
     * @var array
     */
    private $connectionIndex = array();

    /**
     * Instances global
     *
     * @var array
     */
    private static $globalInstances = [];

    /**
     * Get ConnectionPool instance
     * parameter $pipe generally is "pub" or "sub"
     *
     * @param $pipe
     * @return self
     */
    public static function getInstance($pipe)
    {
        if (isset(self::$globalInstances[$pipe]))
        {
            $instance = self::$globalInstances[$pipe];
        }
        else
        {
            self::$globalInstances[$pipe] = $instance = new self;
        }
        return $instance;
    }

    /**
     * Add connection
     * 
     * @param ConnectionInterface $connection
     */
    public function add(ConnectionInterface $connection)
    {
        $identify = (string)$connection;
        $this->connectionIndex[$identify] = $connection;
        $this->connections[] = $connection;
    }
    
    /**
     * Test if has connection
     * 
     * Remember that the sockets are lazy-initialised so we can create
     * connection instances to test with without incurring a socket connection.
     * 
     * @param ConnectionInterface $connectionTest
     * 
     * @return boolean
     */
    public function hasConnection(ConnectionInterface $connectionTest)
    {
        foreach ($this->connections as $connection)
        {
            if ($connection->getSocket() === $connectionTest->getSocket())
            {
                return TRUE;
            }
        }
        return FALSE;
    }

    /**
     * Test if has host for connection
     *
     * @param $host
     * @param $portTcp
     * @param $portHttp
     * @param $partitionID
     *
     * @return boolean
     */
    public function hasHost($host, $portTcp, $portHttp, $partitionID = null)
    {
        $partitionID = is_numeric($partitionID) ? $partitionID : 'N';
        foreach ($this->connections as $connection)
        {
            if ((string)$connection === "{$host}:{$portTcp}/{$portHttp}/-{$partitionID}")
            {
                return TRUE;
            }
        }
        return FALSE;
    }

    /**
     * Find connection from socket/host
     *
     * @param Resource $socket
     * 
     * @return ConnectionInterface|NULL Will return NULL if not found
     */
    public function findConnection($socket)
    {
        foreach ($this->connections as $connection)
        {
            if ($connection->getSocket() === $socket)
            {
                return $connection;
            }
        }
        return NULL;
    }

    /**
     * @param $hosts
     * @return Connection[]
     */
    public function filterConnections($hosts)
    {
        $selected = array();
        foreach ($hosts as $info)
        {
            $partition = is_numeric($info['partition']) ? $info['partition'] : 'N';
            $identify = "{$info['host']}:{$info['ports']['tcp']}/{$info['ports']['http']}/-{$partition}";
            if (isset($this->connectionIndex[$identify]))
            {
                $selected[] = $this->connectionIndex[$identify];
            }
        }
        return $selected;
    }

    /**
     * empty self connections pool
     */
    public function emptyConnections()
    {
        $this->connections = array();
        $this->connectionIndex = array();
    }

    /**
     * Get key of current item as string
     *
     * @return string
     */
    public function key()
    {
        return key($this->connections);
    }

    /**
     * Test if current item valid
     *
     * @return boolean
     */
    public function valid()
    {
        return (current($this->connections) === FALSE) ? FALSE : TRUE;
    }

    /**
     * Fetch current value
     *
     * @return mixed
     */
    public function current()
    {
        return current($this->connections);
    }

    /**
     * Go to next item
     */
    public function next()
    {
        next($this->connections);
    }

    /**
     * Rewind to start
     */
    public function rewind()
    {
        reset($this->connections);
    }

    /**
     * Move to end
     */
    public function end()
    {
        end($this->connections);
    }

    /**
     * Get count of items
     *
     * @return integer
     */
    public function count()
    {
        return count($this->connections);
    }
    
    /**
     * Shuffle connections
     */
    public function shuffle()
    {
        shuffle($this->connections);
    }
}