<?php
/**
 * Config manager
 * User: moyo
 * Date: 19/12/2016
 * Time: 3:17 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Exception\InvalidConfigException;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Exception as SysException;

class Config
{
    use SingleInstance;

    /**
     * @var array
     */
    private $topicMapCaches = [];

    private $globalSetting = [];
    /**
     * Config constructor.
     */
    public function __construct()
    {
        HA::getInstance()->registerEvent(HA::EVENT_RETRYING, [$this, 'cleanWhenSrvRetrying']);
    }

    /**
     * @param $topicNamed
     * @return mixed
     * @throws InvalidConfigException
     */
    public function parseTopicName($topicNamed)
    {
        $topicConfig = $this->getTopicConfig($topicNamed);

        if (isset($topicConfig['topic']))
        {
            return $topicConfig['topic'];
        }
        else
        {
            throw new InvalidConfigException('Parsing topic name failed', 9999);
        }
    }

    /**
     * @param $topicNamed
     * @return array
     * @throws InvalidConfigException
     */
    public function getTopicConfig($topicNamed)
    {
        $topicParsed = $this->convertTopicSign($topicNamed);

        $groupConfig = $this->getTopicGroupConfig($topicParsed);

        if (isset($groupConfig[$topicParsed]))
        {
            return $groupConfig[$topicParsed];
        }
        else
        {
            throw new InvalidConfigException('Missing required topic config ~ '.$topicParsed, 9998);
        }
    }

    /**
     * @param string $topicNamed
     */
    public function clearTopicCache($topicNamed = null)
    {
        if (is_null($topicNamed))
        {
            unset($this->topicMapCaches);
        }
        else
        {
            $groupName = $this->extractTopicGroup($this->convertTopicSign($topicNamed));

            if ($groupName && isset($this->topicMapCaches[$groupName]))
            {
                unset($this->topicMapCaches[$groupName]);
            }
        }
    }

    /**
     * @param $key
     * @param $default
     * @return mixed
     */
    public function getGlobalSetting($key, $default = null)
    {
        return $this->globalSetting[$key] ?: $default;
    }

    public function setGlobalSetting($setting)
    {
        $this->globalSetting = $setting;
    }

    /**
     * @param SysException $e
     */
    public function cleanWhenSrvRetrying(SysException $e)
    {
        $this->clearTopicCache();
    }

    /**
     * @param $topicParsed
     * @return array
     * @throws InvalidConfigException
     */
    private function getTopicGroupConfig($topicParsed)
    {
        $groupName = $this->extractTopicGroup($topicParsed);

        if (isset($this->topicMapCaches[$groupName]))
        {
            $mapping = $this->topicMapCaches[$groupName];
        }
        else
        {
            throw new InvalidConfigException('Missing nsq-topic config', 9997);
        }
        return $mapping;
    }

    /**
     * @param $config array topic config
     */
    public function addTopicConfig($groupName, $config) {
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

        // sharding enables
        $shardingEnables = isset($config['sharding_enabled']) ? $config['sharding_enabled'] : [];

        // rw strategy
        $rwStrategy = isset($config['rw_strategy']) ? $config['rw_strategy'] : [];

        // mixing config
        $mapping = [];

        foreach ($config['topic'] as $topicBiz => $topicNsq)
        {
            $scopeName = $groupName;
            $lookupdCopy = $lookupdPool;
            $shardingMsg = false;

            if (isset($rwStrategy[$topicBiz]))
            {
                $scopeName = $topicBiz;
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

            if (isset($shardingEnables[$topicBiz]))
            {
                $shardingMsg = $shardingEnables[$topicBiz] ? true : false;
            }

            $mapping[$topicBiz] = [
                'group' => $groupName,
                'scope' => $scopeName,
                'name' => $topicBiz,
                'topic' => $topicNsq,
                'sharding' => $shardingMsg,
                'lookups' => $lookupdCopy
            ];
        }
        $this->topicMapCaches[$groupName] = $mapping;
    }

    /**
     * @param $topicName
     * @return string
     */
    private function convertTopicSign($topicName)
    {
        return str_replace('.', '_', $topicName);
    }

    /**
     * @param $topicParsed
     * @return string
     */
    private function extractTopicGroup($topicParsed)
    {
        return substr($topicParsed, 0, strpos($topicParsed, '_') ?: strlen($topicParsed));
    }
}
