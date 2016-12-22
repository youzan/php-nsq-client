<?php
/**
 * Router manager (pub/sub nodes)
 * User: moyo
 * Date: 20/12/2016
 * Time: 5:59 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Exception\MissingRoutesException;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use nsqphp\Lookup\Cluster;

use Exception as SysException;

class Router
{
    use SingleInstance;

    /**
     * @var Config
     */
    private $config = null;

    /**
     * @var Cache
     */
    private $cache = null;

    /**
     * @var array
     */
    private $l2Cache = [];

    /**
     * %s is topic
     * @var string
     */
    private $cLookupResultsKey = 'lookup-results-%s';

    /**
     * @var int
     */
    private $cLookupResultsTTL = 15;

    /**
     * %s is pool
     * @var string
     */
    private $cLookupdNodesKey = 'lookupd-nodes-%s-%s';

    /**
     * @var int
     */
    private $cLookupdNodesTTL = 45;

    /**
     * Router constructor.
     */
    public function __construct()
    {
        HA::getInstance()->registerEvent(HA::EVENT_RETRYING, [$this, 'cleanWhenSrvRetrying']);

        $this->config = Config::getInstance();
        $this->cache = Cache::getInstance();
    }

    /**
     * @param $topic
     * @return array
     * @throws MissingRoutesException
     */
    public function fetchPublishNodes($topic)
    {
        $cKey = 'pub-nodes:'.$topic;

        if (isset($this->l2Cache[$cKey]))
        {
            $nodes = $this->l2Cache[$cKey];
        }
        else
        {
            $nodes = $this->cache->host(
                sprintf($this->cLookupResultsKey, $topic),
                function() use ($topic) {
                    return InstanceMgr::getLookupInstance($topic)->lookupHosts($topic, 'pub');
                },
                $this->config->getGlobalSetting('nsq.mem-cache.lookupResultsTTL', $this->cLookupResultsTTL)
            );

            if (empty($nodes))
            {
                throw new MissingRoutesException('Empty nodes for <'.$topic.'>');
            }

            $this->l2Cache[$cKey] = $nodes;
        }
        return $nodes;
    }

    /**
     * @return string
     */
    public function fetchPublishViaType()
    {
        return php_sapi_name() == 'cli' ? 'tcp' : 'http';
    }

    public function fetchGlobalLookups($topic)
    {
        // TODO all lookup nodes should be getting from here
    }

    /**
     * using "list-lookup" API for discovery
     * ** ONLY WORKS IN HA-Cluster/SQS
     * @param $seedHost
     * @param $seedPort
     * @return array
     */
    public function discoveryViaLookupd($seedHost, $seedPort)
    {
        $cKey = 'disc-results:'.$seedHost.':'.$seedPort;

        if (isset($this->l2Cache[$cKey]))
        {
            $foundNodes = $this->l2Cache[$cKey];
        }
        else
        {
            $foundNodes = $this->cache->host(
                sprintf($this->cLookupdNodesKey, $seedHost, $seedPort),
                function() use ($seedHost, $seedPort) {
                    return (new Cluster($seedHost, $seedPort))->getSlaves();
                },
                $this->config->getGlobalSetting('nsq.mem-cache.lookupdNodesTTL', $this->cLookupdNodesTTL)
            ) ?: [];

            foreach ($foundNodes as $idx => $nodeURL)
            {
                if (filter_var($nodeURL, FILTER_VALIDATE_URL) === false)
                {
                    unset($foundNodes[$idx]);
                }
            }

            $this->l2Cache[$cKey] = $foundNodes;
        }

        return $foundNodes;
    }

    /**
     * @return void
     */
    public function clearCaches()
    {
        $this->cache->clear();
        unset($this->l2Cache);
    }

    /**
     * @param SysException $e
     */
    public function cleanWhenSrvRetrying(SysException $e)
    {
        $this->clearCaches();
    }
}