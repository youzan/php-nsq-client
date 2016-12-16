<?php
/**
 * Nsq api
 * User: moyo
 * Date: 5/7/15
 * Time: 3:45 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Cache\Contract\Protocol\Memory as MemCacheInterface;
use Kdt\Iron\Cache\Foundation\Options as MemCacheOptions;
use Kdt\Iron\Cache\Provider\Memory as MemCacheProvider;

use Kdt\Iron\Config\Live\DCC;

use Kdt\Iron\Queue\Exception\InvalidConfigException;
use Kdt\Iron\Queue\Exception\NonCatchableException;
use Kdt\Iron\Queue\Interfaces\AdapterInterface;
use Kdt\Iron\Queue\Message;

use nsqphp\Connection\Proxy;
use nsqphp\Exception\FailedOnAllNodesException;
use nsqphp\Exception\FailedOnNotLeaderException;
use nsqphp\Exception\LookupException;
use nsqphp\Exception\ShardingStrategyException;
use nsqphp\Exception\TopicNotExistException;
use nsqphp\Lookup\Cluster;
use nsqphp\Lookup\IOSplitter;
use nsqphp\Lookup\LookupInterface;
use nsqphp\Message\Message as NsqMessage;
use nsqphp\Exception\RequeueMessageException;
use nsqphp\nsqphp;

use Config;
use Time;
use Exception;

class Client implements AdapterInterface
{
    /**
     * topic routes
     * @var array
     */
    private $topicRoutes = [];

    /**
     * via routes
     * @var array
     */
    private $viaRoutes = [];

    /**
     * nsq instances
     * @var nsqphp[]
     */
    private $nsqInstances = [];

    /**
     * lookupd instances
     * @var LookupInterface[]
     */
    private $lookupInstances = [];

    /**
     * proxy instances
     * @var Proxy
     */
    private $proxyInstances = [];

    /**
     * nsq logger
     * @var Logger
     */
    private $loggerInstance = null;

    /**
     * @var array
     */
    private $topicMapCaches = [];

    /**
     * @var array
     */
    private $topicTraceSignals = [];

    /**
     * @var bool
     */
    private $topicTraceRealtimeDetect = false;

    /**
     * @var string
     */
    private $lastPoppingTopic = null;

    /**
     * @var int
     */
    private $pubRetryMax = 3;

    /**
     * @var int
     */
    private $pubRetryCount = 0;

    /**
     * @var array
     */
    private $lookupDSNs = [];

    /**
     * @var MemCacheInterface
     */
    private $memCacheInstance = null;

    /**
     * @var string
     */
    private $memCacheApp = '#kdt-iron-queue';

    /**
     * @var string
     */
    private $memCacheModule = 'nsq-client';

    /**
     * %s is topic
     * @var string
     */
    private $mcLookupResultsKey = 'lookup-results-%s';

    /**
     * %s is pool
     * @var string
     */
    private $mcLookupdNodesKey = 'lookupd-nodes-%s';

    /**
     * @var int
     */
    private $mcLookupResultsTTL = 15;

    /**
     * @var int
     */
    private $mcLookupdNodesTTL = 45;

    /**
     * @var string
     */
    private $insTypePUB = '#pub';

    /**
     * @var string
     */
    private $insTypeSUB = '#sub';

    /**
     * @TODO This api should be deprecated
     * @var string
     */
    private $insTypeSTAT = '#stat';

    /**
     * @param $topic
     * @param $message
     * @return array
     */
    public function push($topic, $message)
    {
        return $this->pubRetrying($topic, function () use ($topic, $message) {
            $pubTopic = $this->parseTopic($topic);
            $nsq = $this->findInstance($this->insTypePUB, $this->findTopicUsage($topic), $topic);
            return $nsq->publishTo($this->fetchPubNodes($nsq, $pubTopic, $message), $this->detectPushType())->publish($pubTopic, $this->getNsqMsgObject($pubTopic, $message));
        });
    }

    /**
     * @param $topic
     * @param $messages
     * @return array
     */
    public function bulk($topic, array $messages)
    {
        return $this->pubRetrying($topic, function () use ($topic, $messages) {
            $pubTopic = $this->parseTopic($topic);
            $bag = [];
            foreach ($messages as $message)
            {
                $bag[] = $this->getNsqMsgObject($pubTopic, $message);
            }
            $nsq = $this->findInstance($this->insTypePUB, $this->findTopicUsage($topic), $topic);
            return $nsq->publishTo($this->fetchPubNodes($nsq, $pubTopic), $this->detectPushType())->publish($pubTopic, $bag);
        });
    }

    /**
     * @param $nsqTopic
     * @param $originMsg
     *
     * @return NsqMessage
     */
    private function getNsqMsgObject($nsqTopic, $originMsg)
    {
        $nsqTraceID = 0;

        if (is_object($originMsg) && $originMsg instanceof Message)
        {
            $traceID = $originMsg->getTraceID();
            if ($traceID)
            {
                if (    !  $this->topicTraceRealtimeDetect
                        && isset($this->topicTraceSignals[$nsqTopic])
                        && $this->topicTraceSignals[$nsqTopic]
                )
                {
                    $nsqTraceID = $traceID;
                }
                else
                {
                    try
                    {
                        $dccSwitch = DCC::get(['nsq', 'topic.trace', $nsqTopic]);
                    }
                    catch (Exception $e)
                    {
                        $dccSwitch = 0;
                    }

                    $traceSignal = (int)$dccSwitch ? true : false;

                    $this->topicTraceSignals[$nsqTopic] = $traceSignal;

                    if ($traceSignal)
                    {
                        $nsqTraceID = $traceID;
                    }
                }
            }

            $payload = $originMsg->getPayload();
        }
        else
        {
            $payload = json_encode($originMsg);
        }

        return new NsqMessage($payload, $nsqTraceID);
    }

    /**
     * @param string   $topic
     * @param callable $processor
     * @return array
     * @throws NonCatchableException
     */
    private function pubRetrying($topic, callable $processor)
    {
        try
        {
            $result = call_user_func($processor);
            $this->pubRetryCount = 0;
        }
        catch (Exception $e)
        {
            if ($e instanceof TopicNotExistException || $e instanceof NonCatchableException)
            {
                // throw it
                throw $e;
            }
            else if ($e instanceof LookupException || $e instanceof FailedOnNotLeaderException || $e instanceof FailedOnAllNodesException)
            {
                // retry
                $this->pubRetryCount ++;
                if ($this->pubRetryCount > $this->pubRetryMax)
                {
                    // final failed, make results to caller
                    $this->pubRetryCount = 0;
                }
                else
                {
                    $this->detectAndResetLookupCache($e, $this->insTypePUB);
                    $this->resetPubNodes($this->parseTopic($topic));
                    return $this->pubRetrying($topic, $processor);
                }
            }
            $result = ['success' => 0, 'errors' => [get_class($e).' : '.$e->getMessage()]];
        }
        return $this->makePubResult($topic, $result);
    }

    /**
     * @param $topic
     * @param callable $callback
     * @param $options
     * @return string
     */
    public function pop($topic, callable $callback, array $options = [])
    {
        // topic & channel
        if (is_array($topic))
        {
            list($topic, $channel) = $topic;
        }
        else
        {
            $channel = 'default';
        }
        // flag popping
        $this->lastPoppingTopic = $topic;
        // load config && parsing
        $subTopic = $this->parseTopic($topic);
        // sub
        $subStartTime = Time::stamp();
        $nsq = $this->findInstance($this->insTypeSUB);
        try
        {
            $nsq
            ->subscribe
            (
                $subTopic,
                $channel,
                function (NsqMessage $msg) use ($callback)
                {
                    call_user_func_array($callback, [
                        (new Message(
                            $msg->getId(),
                            $msg->getTimestamp(),
                            $msg->getAttempts(),
                            $msg->getPayload()
                        ))
                            ->setTraceID($msg->getTraceId())
                    ]);
                },
                $options['auto_delete'],
                $options['sub_ordered']
            )
            ->run
            (
                $options['keep_seconds']
            );
        }
        catch (Exception $e)
        {
            // exception observer
            // # custom
            $options['exception_observer'] && call_user_func_array($options['exception_observer'], [$e]);
            // make retry
            if ($options['max_retry'] > 0)
            {
                $options['max_retry'] --;
                // logging
                $this->getLogger()->warn('[IRON] Subscribe retrying('.$options['max_retry'].') : ['.$subTopic.':'.$channel.'] ~ '.$e->getMessage());
                // move keep seconds
                $lastKeepSeconds = $options['keep_seconds'];
                $subCostSeconds = Time::stamp() - $subStartTime;
                $options['keep_seconds'] -= $subCostSeconds;
                // KS checking
                if ($options['keep_seconds'] <= 0 || $options['keep_seconds'] > $lastKeepSeconds)
                {
                    $this->getLogger()->info('[IRON] SUB-KS checking error : ['.$subTopic.':'.$channel.'] ~ ['.$options['keep_seconds'].':'.$lastKeepSeconds.'] ~ fixed');
                    $options['keep_seconds'] = $lastKeepSeconds;
                }
                // lookup cache fix
                $this->detectAndResetLookupCache($e, $this->insTypeSUB);
                // make delay
                sleep($options['retry_delay']);
                // retrying
                return $this->pop([$topic, $channel], $callback, $options);
            }
            else
            {
                // last retry ... abandoning
                return $e->getMessage();
            }
        }
        return false;
    }

    /**
     * exiting pop
     */
    public function stop()
    {
        $nsq = $this->findInstance($this->insTypeSUB);
        $nsq->stop();
    }

    /**
     * @param $messageId
     * @return bool
     */
    public function delete($messageId)
    {
        $nsq = $this->findInstance($this->insTypeSUB);
        return $nsq->deleteMessage($messageId);
    }

    /**
     * make delay
     * @param $seconds
     */
    public function later($seconds)
    {
        throw new RequeueMessageException($seconds * 1000);
    }

    /**
     * make retry
     */
    public function retry()
    {
        throw new RequeueMessageException(1);
    }

    /**
     * close all connections
     */
    public function close()
    {
        $nsq = $this->findInstance($this->insTypeSUB);
        $nsq->close();
    }

    /**
     * 
     * 
     */
    public function stats($topic)
    {
        $res = [];
        
        $pubTopic = $this->parseTopic($topic);
        
        try {
            
            $nsq = $this->findInstance($this->insTypeSTAT, $this->findTopicUsage($topic), $topic);
            
            $hosts = $this->fetchPubNodes($nsq, $pubTopic);
            
            foreach ($hosts as $host) {
                
                $res[$host['host']] = $nsq->node_stats($host['host']);
            }
            
        } catch (Exception $e) {
            
            
        }
        
        return $res;
    }
    
    /**
     * 获取TOPIC
     * @param $name
     * @return string
     * @throws Exception
     */
    private function parseTopic($name)
    {
        $topicConfig = $this->getTopicConfig($this->parseTopicSign($name));
        if (isset($topicConfig['topic']))
        {
            return $topicConfig['topic'];
        }
        else
        {
            throw new InvalidConfigException(9999, 'nsq.topic.resolving.failed');
        }
    }

    /**
     * 处理TOPIC标记
     * @param $name
     * @return string
     */
    private function parseTopicSign($name)
    {
        return str_replace('.', '_', $name);
    }

    /**
     * @param $topicParsed
     * @return array
     * @throws \Exception_System
     */
    private function getTopicConfig($topicParsed)
    {
        $groupConfig = $this->getTopicGroupConfig($topicParsed);
        if (isset($groupConfig[$topicParsed]))
        {
            return $groupConfig[$topicParsed];
        }
        else
        {
            throw new InvalidConfigException(9999, 'missing required topic config');
        }
    }

    /**
     * @param $topicParsed
     * @return array
     * @throws \Exception_System
     */
    private function getTopicGroupConfig($topicParsed)
    {
        $scopeName = substr($topicParsed, 0, strpos($topicParsed, '_') ?: strlen($topicParsed));

        if (isset($this->topicMapCaches[$scopeName]))
        {
            $mapping = $this->topicMapCaches[$scopeName];
        }
        else
        {
            $filePath = RESOURCE_PATH . 'nsq/'.$scopeName.'.php';
            if (is_file($filePath))
            {
                $config = require $filePath;
            }
            else
            {
                throw new InvalidConfigException(9999, 'missing nsq-topic config file');
            }

            // lookupd pool
            $lookupdServers = [];
            $lookupdPool = [];
            $lookupdBiz = $config['lookupd_pool'];
            foreach ($lookupdBiz as $arrayKey => $arrayVal)
            {
                // kv style
                if (is_numeric($arrayKey))
                {
                    // ['global']
                    $lookupdName = $arrayVal;
                    $rwLimit = 'rw';
                }
                else
                {
                    // ['global' => 'rw']
                    $lookupdName = $arrayKey;
                    $rwLimit = $arrayVal;
                }

                $lookupdDSN = '@self';
                $lookupdServers[$lookupdName] = $lookupdDSN;

                // rw pool
                if ($rwLimit == 'r' || $rwLimit == 'rw')
                {
                    $lookupdPool['r'][$lookupdName] = $lookupdDSN;
                }
                if ($rwLimit == 'w' || $rwLimit == 'rw')
                {
                    $lookupdPool['w'][$lookupdName] = $lookupdDSN;
                }
            }

            $rwStrategy = isset($config['rw_strategy']) ? $config['rw_strategy'] : [];

            $mapping = [];

            foreach ($config['topic'] as $topicBiz => $topicNsq)
            {
                $relateGroup = $scopeName;
                $lookupdCopy = $lookupdPool;
                if (isset($rwStrategy[$topicBiz]))
                {
                    $relateGroup = $topicBiz;
                    foreach ($rwStrategy[$topicBiz] as $lookupdName => $rwLimit)
                    {
                        $appendR = $appendW = $removeR = $removeW = false;
                        $lookupdDSN = '@self';
                        switch ($rwLimit)
                        {
                            case 'r':
                                $appendR = true;
                                $removeW = true;
                                break;
                            case 'w':
                                $appendW = true;
                                $removeR = true;
                                break;
                            case 'rw':
                                $appendR = $appendW = true;
                                break;
                            default:
                                $removeR = $removeW = true;
                                break;
                        }
                        if ($appendR && !isset($lookupdCopy['r'][$lookupdName]))
                        {
                            $lookupdCopy['r'][$lookupdName] = $lookupdDSN;
                        }
                        if ($appendW && !isset($lookupdCopy['w'][$lookupdName]))
                        {
                            $lookupdCopy['w'][$lookupdName] = $lookupdDSN;
                        }
                        if ($removeR)
                        {
                            unset($lookupdCopy['r'][$lookupdName]);
                        }
                        if ($removeW)
                        {
                            unset($lookupdCopy['w'][$lookupdName]);
                        }
                    }
                }
                $mapping[$topicBiz] = ['scope' => $scopeName, 'group' => $relateGroup, 'name' => $topicBiz, 'topic' => $topicNsq, 'lookups' => $lookupdCopy];
            }

            $this->topicMapCaches[$scopeName] = $mapping;
        }
        return $mapping;
    }

    /**
     * 自动选择PUB通道
     * @return string // tcp or http
     */
    private function detectPushType()
    {
        if (php_sapi_name() == 'cli')
        {
            return 'tcp';
        }
        else
        {
            return 'http';
        }
    }

    /**
     * 获取多个节点
     * @param $instance
     * @param $topic
     * @param $relateMsg
     * @return array
     * @throws Exception
     */
    private function fetchPubNodes(nsqphp $instance, $topic, $relateMsg = null)
    {
        if (isset($this->topicRoutes[$topic]))
        {
            $nodes = $this->topicRoutes[$topic];
        }
        else
        {
            $hostsTopic = $this->getMemCache()->host(
                sprintf($this->mcLookupResultsKey, $topic),
                function() use ($instance, $topic) {
                    return $instance->getNsLookup()->lookupHosts($topic, 'pub');
                },
                $this->getGlobalSetting('nsq.mem-cache.lookupResultsTTL', $this->mcLookupResultsTTL)
            );
            if ($hostsTopic)
            {
                $this->topicRoutes[$topic] = $nodes = $hostsTopic;
            }
            else
            {
                throw new Exception('nsq.lookupd.nodes.empty');
            }
        }

        // sharding limits
        if ($relateMsg && is_object($relateMsg) && $relateMsg instanceof Message)
        {
            $proof = $relateMsg->getShardingProof();
            if ($proof)
            {
                $mapPrefix = 'pt_';
                $knownPartitions = null;
                $partitionNodes = [];
                foreach ($nodes as $node)
                {
                    if (isset($node['meta']))
                    {
                        $partitions = isset($node['meta']['partitions']) ? $node['meta']['partitions'] : -1;
                        if ($partitions > 0)
                        {
                            if (is_null($knownPartitions))
                            {
                                $knownPartitions = $partitions;
                            }

                            if ($knownPartitions != $partitions)
                            {
                                throw new ShardingStrategyException('Conflict partitions info');
                            }

                            $partitionNodes[$mapPrefix.$node['partition']] = $node;
                        }
                    }
                }
                if ($knownPartitions && $partitionNodes)
                {
                    $targetSlot = $mapPrefix.(string)($proof % $knownPartitions);
                    if (isset($partitionNodes[$targetSlot]))
                    {
                        $nodes = [$partitionNodes[$targetSlot]];
                    }
                    else
                    {
                        throw new ShardingStrategyException('Target partition not found');
                    }
                }
                else
                {
                    throw new ShardingStrategyException('No available partitions');
                }
            }
        }

        return $nodes;
    }

    /**
     * @param $topic
     */
    private function resetPubNodes($topic)
    {
        $this->getMemCache()->del(sprintf($this->mcLookupResultsKey, $topic));
        unset($this->topicRoutes[$topic]);
    }

    /**
     * @TODO temp action for operate lookup to offline
     * @param Exception $exception
     * @param string $insType
     */
    private function detectAndResetLookupCache(Exception $exception, $insType)
    {
        if ($exception instanceof LookupException)
        {
            unset($this->topicMapCaches);

            if (isset($this->nsqInstances[$insType]))
            {
                unset($this->nsqInstances[$insType]);
            }

            unset($this->lookupInstances);

            $memCacheKeys = array_keys($this->lookupDSNs);

            foreach ($memCacheKeys as $clusterName)
            {
                $this->getMemCache()->del(sprintf($this->mcLookupdNodesKey, $clusterName));
            }

            unset($this->lookupDSNs);
        }
    }

    /**
     * 获取可用的nsqd服务
     * @param string $insType
     * @param string $usage
     * @param string $topic
     * @return nsqphp
     * @throws Exception
     * @throws \Exception_System
     */
    private function findInstance($insType, $usage = 'via-socket', $topic = null)
    {
        if (is_null($topic) && $insType == $this->insTypeSUB)
        {
            if (is_null($this->lastPoppingTopic))
            {
                throw new Exception('nsq.popping-topic.unknown');
            }
            else
            {
                $topic = $this->lastPoppingTopic;
            }
        }
        $topicConfig = $this->getTopicConfig($this->parseTopicSign($topic));
        if (isset($this->nsqInstances[$insType][$usage][$topicConfig['group']]))
        {
            $instance = $this->nsqInstances[$insType][$usage][$topicConfig['group']];
        }
        else
        {
            $this->nsqInstances[$insType][$usage][$topicConfig['group']] = $instance = $this->createInstance($this->fetchLookupd($usage, $topicConfig), $this->fetchProxy($usage));
        }
        return $instance;
    }

    /**
     * @param $topic
     * @return string
     */
    private function findTopicUsage($topic)
    {
        if (isset($this->viaRoutes[$topic]))
        {
            $via = $this->viaRoutes[$topic];
        }
        else
        {
            $pipe = 'origin';
            // temporary : ignore proxy
            if (Config::get('nsq.proxy.overall') || in_array($this->parseTopicSign($topic), []))
            {
                if ($this->detectPushType() == 'http')
                {
                    $pipe = 'proxy';
                }
            }
            $this->viaRoutes[$topic] = $via = 'via-'.$pipe;
        }
        return $via;
    }

    /**
     * @param $config
     * @return array
     */
    private function getLookupdDSNs($config)
    {
        $scope = $config['scope'];
        $topic = $config['topic'];
        $lookupLists = $config['lookups'];

        $DSNsPool = [];

        $pipes = ['r', 'w'];
        foreach ($pipes as $pipe)
        {
            if (isset($lookupLists[$pipe]))
            {
                $DSNsDynamic = [];
                $DSNsStatic = [];

                $lookupChannel = $lookupLists[$pipe];
                foreach ($lookupChannel as $clusterName => $providerDSN)
                {
                    if ($providerDSN == '@self')
                    {
                        list($provideDSNs, $dynamic) = $this->getLookupdDSN($clusterName, $pipe, $scope, $topic);
                        if ($dynamic)
                        {
                            $DSNsDynamic[$clusterName] = $provideDSNs;
                        }
                        else
                        {
                            $DSNsStatic[$clusterName] = $provideDSNs;
                        }
                    }
                    else
                    {
                        $DSNsStatic[$clusterName] = [$providerDSN];
                    }
                }

                if ($DSNsDynamic)
                {
                    // ignore static config
                    $DSNsPool[$pipe] = $DSNsDynamic;
                }
                else
                {
                    $DSNsPool[$pipe] = $DSNsStatic;
                }
            }
        }

        return $DSNsPool;
    }

    /**
     * @param $cluster
     * @param $pipe
     * @param $scope
     * @param $topic
     *
     * @return string
     */
    private function getLookupdDSN($cluster, $pipe = null, $scope = null, $topic = null)
    {
        if (isset($this->lookupDSNs[$cluster]))
        {
            return $this->lookupDSNs[$cluster];
        }

        $viaDynamic = false;

        $extDSNs = [];

        $config = $this->getLookupdBalanced(Config::get('nsq.server.lookupd.'.$cluster));

        if (is_numeric(strpos($config, '://')))
        {
            // new syntax like "http://lookupd.domain.dns:4161"
            $mainDSN = $config;
        }
        else
        {
            // old syntax like "http:lookupd.domain.dns:4161"
            list($lgProtocol, $lgPath, $lgExt) = explode(':', $config);
            // upgrade to new syntax
            $mainDSN = sprintf('%s://%s:%s', $lgProtocol, $lgPath, $lgExt);
        }

        $this->getLogger()->debug('GOT Lookupd DSN ~ origin ~ '.$cluster.' -> '.$mainDSN);

        $DSNParsed = parse_url($mainDSN);

        if ($DSNParsed['scheme'] == 'dcc')
        {
            $parameters = [];
            parse_str($DSNParsed['query'], $parameters);

            list($app, $module) = explode('~', $parameters['query']);

            $clusterSIGN = '';
            $cSepPos = strpos($cluster, '-');
            if (is_numeric($cSepPos))
            {
                $clusterSIGN = '_'.substr($cluster, $cSepPos + 1);
            }

            $defaultKey = '##_default'.$clusterSIGN;
            $topicKey = $topic;

            $clientRole = $pipe == 'r' ? 'consumer' : 'producer';

            $cloudStrategy = DCC::gets([sprintf($app, $scope), sprintf($module, $clientRole)], [$defaultKey, $topicKey]);

            $usedStrategy =
                isset($cloudStrategy[$topicKey])
                    ? $cloudStrategy[$topicKey]
                    : (isset($cloudStrategy[$defaultKey]) ? $cloudStrategy[$defaultKey] : null);

            if ($usedStrategy)
            {
                $usedStrategy = json_decode($usedStrategy, TRUE);

                $producerTargets = [];

                if (isset($usedStrategy['previous']) && $usedStrategy['previous'])
                {
                    if ($clientRole == 'consumer')
                    {
                        $extDSNs[] = $this->getLookupdBalanced($usedStrategy['previous']);
                    }

                    if ($clientRole == 'producer')
                    {
                        $producerTargets['previous'] = $usedStrategy['previous'];
                    }
                }

                if (isset($usedStrategy['current']) && $usedStrategy['current'])
                {
                    if ($clientRole == 'consumer')
                    {
                        $extDSNs[] = $this->getLookupdBalanced($usedStrategy['current']);
                    }

                    if ($clientRole == 'producer')
                    {
                        $producerTargets['current'] = $usedStrategy['current'];
                    }
                }

                if ($producerTargets)
                {
                    if (isset($usedStrategy['gradation']) && $usedStrategy['gradation'])
                    {
                        $grayHosts = $usedStrategy['gradation'];
                        $localHost = gethostname();

                        if (isset($grayHosts[$localHost]))
                        {
                            $grayRule = $grayHosts[$localHost];
                        }
                        else if (isset($grayHosts['*']))
                        {
                            $grayRule = $grayHosts[$localHost];
                        }
                        else
                        {
                            $grayRule = null;
                        }

                        if ($grayRule)
                        {
                            $grayHit = false;
                            if (isset($grayRule['percent']))
                            {
                                $grayPercentK = round($grayRule['percent'], 3) * 1000;
                                if ($grayPercentK <= rand(0, 100000))
                                {
                                    $grayHit = true;
                                }
                            }

                            if ($grayHit)
                            {
                                $extDSNs[] = $this->getLookupdBalanced($producerTargets['current']);
                            }
                            else
                            {
                                $extDSNs[] = $this->getLookupdBalanced($producerTargets['previous']);
                            }
                        }
                        else
                        {
                            $extDSNs[] = $this->getLookupdBalanced($producerTargets['previous']);
                        }
                    }
                    else
                    {
                        if (isset($producerTargets['current']))
                        {
                            $extDSNs[] = $this->getLookupdBalanced($producerTargets['current']);
                        }
                        else if (isset($producerTargets['previous']))
                        {
                            $extDSNs[] = $this->getLookupdBalanced($producerTargets['previous']);
                        }
                    }
                }
            }

            if ($extDSNs)
            {
                $viaDynamic = true;
                if (count($extDSNs) == 1)
                {
                    $mainDSN = current($extDSNs);
                    $extDSNs = [];
                }
            }
            else
            {
                // -> fallback
                if (isset($parameters['fallback']))
                {
                    $mainDSN = $parameters['fallback'];
                }
            }
        }

        if (empty($extDSNs))
        {
            // for SQS-HA

            list($_, $host, $port) = explode(':', $mainDSN);

            // force to discovery
            $discNodes = $this->getMemCache()->host(
                sprintf($this->mcLookupdNodesKey, $cluster),
                function() use ($host, $port) {
                    return (new Cluster($host, $port))->getSlaves();
                },
                $this->getGlobalSetting('nsq.mem-cache.lookupdNodesTTL', $this->mcLookupdNodesTTL)
            );
            if ($discNodes)
            {
                $picked = $discNodes[rand(0, count($discNodes) - 1)];
                $parsed = parse_url($picked);
                if (isset($parsed['scheme']) && isset($parsed['host']))
                {
                    $mainDSN = implode(':', [$parsed['scheme'], $parsed['host'], isset($parsed['port']) ? $parsed['port'] : 80]);
                    $this->getLogger()->debug('GOT Lookupd DSN ~ discovered ~ '.$cluster.' -> '.$mainDSN);
                }
            }
        }

        if ($mainDSN)
        {
            $mergedDSNs = $extDSNs ? array_merge([$mainDSN], $extDSNs) : [$mainDSN];
            $this->lookupDSNs[$cluster] = [$mergedDSNs, $viaDynamic];
        }
        else
        {
            $mergedDSNs = [];
        }

        return [$mergedDSNs, $viaDynamic];
    }

    /**
     * @param $pool
     * @return string
     * @throws InvalidConfigException
     */
    private function getLookupdBalanced($pool)
    {
        if (is_string($pool))
        {
            return $pool;
        }
        elseif (is_array($pool))
        {
            return $pool[rand(0, count($pool) - 1)];
        }
        else
        {
            throw new InvalidConfigException('Illegal lookupd pool info');
        }
    }

    /**
     * 获取lookupd路由服务
     * @param $usage
     * @param $topicConfig
     * @throws Exception
     * @return LookupInterface
     */
    private function fetchLookupd($usage, $topicConfig)
    {
        if (isset($this->lookupInstances[$usage][$topicConfig['group']]))
        {
            $instance = $this->lookupInstances[$usage][$topicConfig['group']];
        }
        else
        {
            $IOSplitter = new IOSplitter();
            if (
                $IOSplitter->registerLogger($this->getLogger())
                &&
                $IOSplitter->registerProxy($this->fetchProxy($usage))
                &&
                $IOSplitter->registerLookupd($this->getLookupdDSNs($topicConfig))
            )
            {
                $this->lookupInstances[$usage][$topicConfig['group']] = $instance = $IOSplitter;
            }
            else
            {
                throw new Exception('nsq.lookupd.server.illegal');
            }
        }
        return $instance;
    }

    /**
     * @param $usage
     * @return Proxy
     */
    private function fetchProxy($usage)
    {
        if (isset($this->proxyInstances[$usage]))
        {
            $proxy = $this->proxyInstances[$usage];
        }
        else
        {
            $instance = $usage == 'via-proxy' ? new Proxy(Config::get('nsq.proxy.host'), Config::get('nsq.proxy.port')) : new Proxy();
            $instance->setLogger($this->getLogger());
            $this->proxyInstances[$usage] = $proxy = $instance;
        }
        return $proxy;
    }

    /**
     * 获取nsqd实例
     * @param LookupInterface $lookupd
     * @param Proxy $proxy
     * @return nsqphp
     */
    private function createInstance(LookupInterface $lookupd, Proxy $proxy)
    {
        $nsq = new nsqphp($lookupd, null, null, $this->getLogger());
        $nsq->setProxy($proxy);
        $nsq->setPubNodesPriority(Config::get('nsq.priority') ?: []);
        $warningSet = Config::get('nsq.monitor.msg-bag');
        if ($warningSet && isset($warningSet['nums']) && isset($warningSet['size']))
        {
            $nsq->setPubBodyWarning($warningSet['nums'], $warningSet['size']);
        }
        return $nsq;
    }

    /**
     * @return Logger
     */
    private function getLogger()
    {
        if (is_null($this->loggerInstance))
        {
            $this->loggerInstance = new Logger();
        }
        return $this->loggerInstance;
    }

    /**
     * @return MemCacheInterface
     */
    private function getMemCache()
    {
        if (is_null($this->memCacheInstance))
        {
            $this->memCacheInstance = MemCacheProvider::getInstance($this->memCacheApp, $this->memCacheModule, MemCacheOptions::MEMORY_ALLOW_LEGACY);
        }
        return $this->memCacheInstance;
    }

    /**
     * @param $key
     * @param $default
     * @return mixed
     */
    private function getGlobalSetting($key, $default = null)
    {
        return Config::get($key) ?: $default;
    }

    /**
     * @param $topic
     * @param $result
     * @return array
     */
    private function makePubResult($topic, array $result)
    {
        // check result
        $error_code = -1;
        $error_message = '';
        if ($result['success'])
        {
            $error_code = 0;
        }
        else if ($result['errors'])
        {
            $error_code = 1;
            $error_message = implode('|', $result['errors']);
            // logging
            $this->getLogger()->error('[IRON] Actual failed via (PUB) : ['.$topic.'] ~ ' . $error_message);
        }
        // return result
        return [
            'error_code' => $error_code,
            'error_message' => $error_message
        ];
    }
}