<?php
/**
 * DSN translate
 * User: moyo
 * Date: 20/12/2016
 * Time: 7:32 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Adapter\Nsq\Feature\DCCLookupd;
use Kdt\Iron\Queue\Exception\InvalidConfigException;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Exception as SysException;

class DSN
{
    use SingleInstance;

    /**
     * @var Config
     */
    private $config = null;

    /**
     * @var array
     */
    private $cachedDSNs = [];

    /**
     * DSN constructor.
     */
    public function __construct()
    {
        HA::getInstance()->registerEvent(HA::EVENT_RETRYING, [$this, 'cleanWhenSrvRetrying']);

        $this->config = Config::getInstance();
    }

    /**
     * @param $lookupList
     * @param $topicNamed
     * @param $usingScene
     * @return array
     */
    public function translate($lookupList, $topicNamed = null, $usingScene = null)
    {
        $DSNs = [];

        $pipes = ['r', 'w'];
        foreach ($pipes as $pipe)
        {
            if (isset($lookupList[$pipe]))
            {
                $staticDSNs = $dynamicDSNs = [];

                $lookupChannel = $lookupList[$pipe];
                foreach ($lookupChannel as $clusterName => $providerDSN)
                {
                    if ($providerDSN == '@self')
                    {
                        list($provideDSNs, $viaDynamic) = $this->getLookupdDSN($clusterName, $topicNamed, $usingScene);
                        if ($viaDynamic)
                        {
                            $dynamicDSNs[$clusterName] = $provideDSNs;
                        }
                        else
                        {
                            $staticDSNs[$clusterName] = $provideDSNs;
                        }
                    }
                    else
                    {
                        $staticDSNs[$clusterName] = [$providerDSN];
                    }
                }

                // ignore staticDSNs if we got dynamicDSNs from any cluster
                $DSNs[$pipe] = $dynamicDSNs ?: $staticDSNs;
            }
        }

        return $DSNs;
    }

    /**
     * @return void
     */
    public function clearCaches()
    {
        unset($this->cachedDSNs);
    }

    /**
     * @param SysException $e
     */
    public function cleanWhenSrvRetrying(SysException $e)
    {
        $this->clearCaches();
    }

    /**
     * @param $clusterName
     * @param $topicNamed
     * @param $usingScene
     * @return array
     */
    private function getLookupdDSN($clusterName, $topicNamed = null, $usingScene = null)
    {
        if (isset($this->cachedDSNs[$clusterName]))
        {
            return $this->cachedDSNs[$clusterName];
        }

        $config = $this->balancedPicking($this->config->getGlobalSetting('nsq.server.lookupd.'.$clusterName));

        if (is_numeric(strpos($config, '://')))
        {
            // new syntax like "http://lookupd.domain.dns:4161"
            $bootDSN = $config;
        }
        else
        {
            // old syntax like "http:lookupd.domain.dns:4161"
            list($lgProtocol, $lgPath, $lgExt) = explode(':', $config);
            // upgrade to new syntax
            $bootDSN = sprintf('%s://%s:%s', $lgProtocol, $lgPath, $lgExt);
        }

        $staticResults = $dynamicResults = [];

        $bootParsed = parse_url($bootDSN);
        if ($bootParsed['scheme'] == 'dcc')
        {
            $dynamicResults = DCCLookupd::getInstance()->parsing($clusterName, $bootParsed, $topicNamed, $usingScene);
        }
        else
        {
            $discoveredResults = Router::getInstance()->discoveryViaLookupd($bootParsed['host'], $bootParsed['port'] ?: 80);
            $staticResults = $discoveredResults ?: [$bootDSN];
        }

        $this->cachedDSNs[$clusterName] = $finalResults = [$dynamicResults ?: $staticResults, $dynamicResults ? true : false];

        return $finalResults;
    }

    /**
     * @param $pool
     * @return string
     * @throws InvalidConfigException
     */
    private function balancedPicking($pool)
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
            throw new InvalidConfigException('Illegal lookupd pool info', 9996);
        }
    }
}