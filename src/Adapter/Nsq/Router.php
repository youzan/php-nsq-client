<?php
/**
 * Router manager (pub/sub nodes)
 * User: moyo
 * Date: 20/12/2016
 * Time: 5:59 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Exception\MissingRoutesException;
use Kdt\Iron\Queue\Exception\ShardingStrategyException;
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

    // /**
    //  * @var Cache
    //  */
    //private $cache = null;

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
        //$this->cache = Cache::getInstance();
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
            // TODO: cache
            $nodes = InstanceMgr::getLookupInstance($topic, 'pub')->lookupHosts($this->config->parseTopicName($topic), 'pub');

            if (empty($nodes))
            {
                throw new MissingRoutesException('Empty nodes for <'.$topic.'>');
            }

            $this->l2Cache[$cKey] = $nodes;
        }
        return $nodes;
    }

    /**
     * @param $topic
     * @param $partition
     * @return array
     * @throws MissingRoutesException
     * @throws ShardingStrategyException
     */
    public function fetchSubscribeNodes($topic, $partition = null)
    {
        $nodes = InstanceMgr::getLookupInstance($topic, 'sub')->lookupHosts($this->config->parseTopicName($topic), 'sub');

        if (empty($nodes))
        {
            throw new MissingRoutesException('Empty nodes for <'.$topic.'>');
        }

        if (is_numeric($partition))
        {
            $found = null;
            foreach ($nodes as $node)
            {
                if (isset($node['partition']) && $node['partition'] == $partition)
                {
                    $found = $node;
                    break;
                }
            }

            if ($found)
            {
                $nodes = [$found];
            }
            else
            {
                throw new ShardingStrategyException('Custom partition not found');
            }
        }

        return $nodes;
    }

    /**
     * @return string
     */
    public function fetchPublishViaType()
    {
        if (defined('K_QUEUE_FORCE_PUB_VIA_TYPE'))
        {
            return K_QUEUE_FORCE_PUB_VIA_TYPE;
        }

        return php_sapi_name() == 'cli' ? 'tcp' : 'http';
    }

    /**
     * @param $topic
     * @param $scene
     * @return array
     */
    public function fetchGlobalLookups($topic, $scene = 'mix')
    {
        $cKey = 'global-lookups:'.$topic.':'.$scene;

        if (isset($this->l2Cache[$cKey]))
        {
            $lookups = $this->l2Cache[$cKey];
        }
        else
        {
            $config = $this->config->getTopicConfig($topic);
            $this->l2Cache[$cKey] = $lookups = DSN::getInstance()->translate($config['lookups'], $config['name'], $scene);
        }

        return $lookups;
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
            try
            {
                $foundNodes = [];
                // TODO: cache
                $dynamicNodes = (new Cluster($seedHost, $seedPort))->getSlaves() ?: [];

                shuffle($dynamicNodes);
                foreach ($dynamicNodes as $idx => $nodeURL)
                {
                    if (filter_var($nodeURL, FILTER_VALIDATE_URL))
                    {
                        $foundNodes[] = $nodeURL;
                        break;
                    }
                }

                $this->l2Cache[$cKey] = $foundNodes;
            }
            catch (SysException $e)
            {
                $foundNodes = [];
            }
        }

        return $foundNodes;
    }

    /**
     * @return void
     */
    public function clearCaches()
    {
        //$this->cache->clear();
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
