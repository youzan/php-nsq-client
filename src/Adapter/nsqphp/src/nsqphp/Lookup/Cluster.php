<?php
/**
 * Cluster lookupdS ~ for SQS-HA
 * User: moyo
 * Date: 28/11/2016
 * Time: 3:49 PM
 */

namespace nsqphp\Lookup;

use nsqphp\Connection\HTTP;

class Cluster
{
    /**
     * @var string
     */
    private $remoteAPI = '/listlookup';

    /**
     * @var int
     */
    private $connectTimeout = 1;

    /**
     * @var int
     */
    private $responseTimeout = 2;

    /**
     * @var null
     */
    private $seed = null;

    /**
     * @var null
     */
    private $master = null;

    /**
     * @var array
     */
    private $slaves = [];

    /**
     * Cluster constructor.
     * @param $seedHost
     * @param $seedPort
     */
    public function __construct($seedHost, $seedPort = 4161)
    {
        $this->seed = 'http://'.$seedHost.':'.$seedPort;
        $this->remoteLists();
    }

    /**
     * @return string
     */
    public function getMaster()
    {
        return $this->master;
    }

    /**
     * @return string[]
     */
    public function getSlaves()
    {
        // currently slaves equal all lookupd nodes
        return $this->slaves;
    }

    /**
     * listLookup
     */
    private function remoteLists()
    {
        $url = $this->seed . $this->remoteAPI;

        $timeout = [
            CURLOPT_CONNECTTIMEOUT => $this->connectTimeout,
            CURLOPT_TIMEOUT        => $this->responseTimeout,
        ];

        list($error, $result) = HTTP::get($url, $timeout);

        if ($error)
        {
            // some thing error
        }
        else
        {
            $data = json_decode($result, TRUE);

            if (isset($data['lookupdleader']) && isset($data['lookupdnodes']))
            {
                // master
                $leader = $data['lookupdleader'];
                $this->master = 'http://'.$leader['NodeIP'].':'.$leader['HttpPort'];

                // slaves
                foreach ($data['lookupdnodes'] as $slave)
                {
                    $this->slaves[] = 'http://'.$slave['NodeIP'].':'.$slave['HttpPort'];
                }
            }
        }
    }
}