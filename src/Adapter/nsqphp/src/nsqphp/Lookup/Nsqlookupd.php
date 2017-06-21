<?php

namespace nsqphp\Lookup;

use nsqphp\Connection\HTTP;
use nsqphp\Connection\Proxy;
use nsqphp\Exception\LookupException;
use nsqphp\Logger\LoggerInterface;

/**
 * Represents nsqlookupd and allows us to find machines we need to talk to
 * for a given topic
 */
class Nsqlookupd implements LookupInterface
{
    /**
     * Hosts to connect to, incl. :port
     * 
     * @var array
     */
    private $hosts;

    /**
     * Logger instance
     *
     * @var LoggerInterface
     */
    private $logger;

    /**
     * With proxy
     *
     * @var Proxy
     */
    private $proxy;
    
    /**
     * Connection timeout, in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Response timeout
     * 
     * @var float
     */
    private $responseTimeout;
    
    /**
     * Constructor
     * 
     * @param string|array $hosts Single host:port, many host:port with commas,
     *      or an array of host:port, of nsqlookupd servers to talk to
     *      (will default to localhost)
     * @param float $connectionTimeout In seconds
     * @param float $responseTimeout In seconds
     */
    public function __construct($hosts = NULL, $connectionTimeout = 1.0, $responseTimeout = 2.0)
    {
        if ($hosts === NULL) {
            $this->hosts = array('localhost:4161');
        } elseif (is_array($hosts)) {
            $this->hosts = $hosts;
        } else {
            $this->hosts = explode(',', $hosts);
        }
        $this->connectionTimeout = $connectionTimeout;
        $this->responseTimeout = $responseTimeout;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @param $proxy
     */
    public function setProxy(Proxy $proxy)
    {
        $this->proxy = $proxy;
    }
    
    /**
     * Lookup hosts for a given topic
     * 
     * @param string $topic
     *
     * @param string $sp
     *
     * @throws LookupException If we cannot talk to / get back invalid response
     *      from nsqlookupd
     * 
     * @return array Should return array [] = host:port
     */
    public function lookupHosts($topic = null, $sp = null)
    {
        if (is_null($topic)) throw new LookupException('lookup for all-nodes is deprecated');

        $access = $sp == 'pub' ? 'w' : 'r';

        $ret = $this->proxy->lookupNodes($this->hosts, function ($hosts) use($topic, $access) {
            return $this->lookupNodes($hosts, 'lookup?topic='.$topic.'&metainfo=true&access='.$access);
        });
        return $ret;
    }

    /**
     * Lookup hosts for all available nodes
     *
     * @param $lookupdNodes
     * @param $urlQuery
     *
     * @throws LookupException If we cannot talk to / get back invalid response
     *      from nsqlookupd
     *
     * @return array Should return array [] = host:port
     */
    private function lookupNodes($lookupdNodes, $urlQuery)
    {
        $nsqdNodes = array();
        $lookupdMeta = array();
        $partitionNodes = array();

        foreach ($lookupdNodes as $host)
        {
            // init meta info
            isset($lookupdMeta[$host]) || $lookupdMeta[$host] = [];
            $meta = &$lookupdMeta[$host];

            // ensure host; otherwise go with default (:4161)
            if (strpos($host, ':') === FALSE) {
                $host .= ':4161';
            }

            $url = "http://{$host}/".$urlQuery;

            $timeout = [
                CURLOPT_CONNECTTIMEOUT => $this->connectionTimeout,
                CURLOPT_TIMEOUT        => $this->responseTimeout,
            ];

            list($error, $result) = HTTP::get($url, $timeout);

            if ($error)
            {
                list($errNo, $errMsg) = $error;

                $this->logger && $this->logger->warn('Errors while lookup query : '.$url.' -> '.$errNo.' # '.$errMsg);

                throw new LookupException('lookupd - '.$url.' : '.$errMsg, $errNo);
            }
            else
            {
                $this->logger && $this->logger->debug('Success for lookup query : '.$url);
            }

            $data = json_decode($result, TRUE);

            // get meta info
            if (isset($data['meta']))
            {
                $meta['partitions'] = $data['meta']['partition_num'];
                $meta['replicas'] = $data['meta']['replica'];
                $meta['extend_support'] = isset($data['meta']['extend_support']) ? $data['meta']['extend_support'] : false;
            }

            // init
            if (isset($data['partitions']))
            {
                $nsqdNodes = $this->mergeNsqNodes($nsqdNodes, $data['partitions'], $meta, TRUE, $partitionNodes);
            }

            // merge
            if (isset($data['producers']))
            {
                $nsqdNodes = $this->mergeNsqNodes($nsqdNodes, $data['producers'], $meta, FALSE, $partitionNodes);
            }
        }

        return array_values($nsqdNodes);
    }

    /**
     * @param $previousList
     * @param $nodes
     * @param $meta
     * @param $viaPartition
     * @param $partitionNodes
     * @return array
     */
    private function mergeNsqNodes($previousList, $nodes, &$meta, $viaPartition, &$partitionNodes)
    {
        $mergedList = $previousList;

        foreach ($nodes as $pKey => $pData)
        {
            if (is_numeric($pKey) && $pKey < 0)
            {
                // negative number only in merged cluster (old & new) and maybe never be used
                continue;
            }

            $hostIdx = "{$pData['broadcast_address']}:{$pData['tcp_port']}/{$pData['http_port']}";

            if ($viaPartition)
            {
                isset($partitionNodes[$hostIdx]) || $partitionNodes[$hostIdx] = 'exists';
            }
            else
            {
                if (isset($partitionNodes[$hostIdx]))
                {
                    // ignore producer node if we found it in partition nodes
                    continue;
                }
            }

            $isCluster = $viaPartition ? true : false;
            $partitionID = $viaPartition ? (string)$pKey : null;

            $pid = (is_numeric($partitionID) ? $partitionID : 'N');
            $listIdx = "{$hostIdx}/-{$pid}";

            if (!isset($mergedList[$listIdx]))
            {
                $mergedList[$listIdx] = [
                    'meta' => &$meta,
                    'host' => $pData['broadcast_address'],
                    'cluster' => $isCluster,
                    'partition' => $partitionID,
                    'ports' => [
                        'tcp' => $pData['tcp_port'],
                        'http' => $pData['http_port']
                    ]
                ];
            }
        }

        return $mergedList;
    }
}
