<?php
/**
 * Instance manager
 * User: moyo
 * Date: 19/12/2016
 * Time: 5:21 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Exception\ServiceInitializationException;
use Kdt\Iron\Queue\Exception\UnknownSubInstanceException;

use nsqphp\Connection\Proxy;
use nsqphp\Lookup\IOSplitter;
use nsqphp\Lookup\LookupInterface;
use nsqphp\nsqphp;

use Exception as SysException;

class InstanceMgr
{
    /**
     * @var bool
     */
    private static $HARegistered = false;

    /**
     * @var array
     */
    private static $nsqInstances = [];

    /**
     * @var array
     */
    private static $lookupInstances = [];

    /**
     * @var array
     */
    private static $proxyInstances = [];

    /**
     * @var Logger
     */
    private static $loggerInstance = null;

    /**
     * @var array
     */
    private static $pubRouteConfirms = [];

    /**
     * @var null
     */
    private static $lastSubTopic = null;

    /**
     * register HA callback
     */
    private static function initHASupport()
    {
        if (self::$HARegistered == false)
        {
            HA::getInstance()->registerEvent(HA::EVENT_RETRYING, [__CLASS__, 'cleanWhenSrvRetrying']);
            self::$HARegistered = true;
        }
    }

    /**
     * @param $topic
     * @return nsqphp
     */
    public static function getPubInstance($topic)
    {
        self::initHASupport();

        $topicConfig = self::getConfig()->getTopicConfig($topic);

        $nsqd = self::touchNsqdInstance('pub', self::getStreamingPipe($topic), $topicConfig);

        if (false === isset(self::$pubRouteConfirms[$topic]))
        {
            $nsqd->publishTo(
                self::getConfig()->parseTopicName($topic),
                Router::getInstance()->fetchPublishNodes($topic),
                Router::getInstance()->fetchPublishViaType()
            );
            self::$pubRouteConfirms[$topic] = 'yes';
        }

        return $nsqd;
    }

    /**
     * SUB instance must listen ONE topic in ONE process
     * ELSE will make some error (e.g. lookups and etc.)
     * @param $topic
     * @return nsqphp
     * @throws UnknownSubInstanceException
     */
    public static function getSubInstance($topic = null)
    {
        self::initHASupport();

        if (is_null($topic))
        {
            if (is_null(self::$lastSubTopic))
            {
                throw new UnknownSubInstanceException('Unknown popping topic');
            }
            else
            {
                $topic = self::$lastSubTopic;
            }
        }
        else
        {
            self::$lastSubTopic = $topic;
        }

        $topicConfig = self::getConfig()->getTopicConfig($topic);

        return self::touchNsqdInstance('sub', self::getStreamingPipe($topic), $topicConfig);
    }

    /**
     * @param $topic
     * @param $scene
     * @return LookupInterface
     */
    public static function getLookupInstance($topic, $scene = 'mix')
    {
        self::initHASupport();

        $topicConfig = self::getConfig()->getTopicConfig($topic);

        return self::touchLookupdInstance(self::getStreamingPipe($topic), $topicConfig, $scene);
    }

    /**
     * @return Logger
     */
    public static function getLoggerInstance()
    {
        if (is_null(self::$loggerInstance))
        {
            self::$loggerInstance = new Logger();
        }
        return self::$loggerInstance;
    }

    /**
     * @param SysException $e
     */
    public static function cleanWhenSrvRetrying(SysException $e)
    {
        self::$lookupInstances = [];
        self::$nsqInstances = [];
        self::$pubRouteConfirms = [];
    }

    /**
     * @return Config
     */
    private static function getConfig()
    {
        return Config::getInstance();
    }

    /**
     * @param $topic
     * @return string
     */
    private static function getStreamingPipe($topic)
    {
        $pipe = 'via-origin';

        // should be different pipe in proxy mode

        return $pipe;
    }

    /**
     * @param $type
     * @param $pipe
     * @param $config
     * @return nsqphp
     */
    private static function touchNsqdInstance($type, $pipe, $config)
    {
        $scope = $config['scope'];

        if (isset(self::$nsqInstances[$type][$pipe][$scope]))
        {
            $nsqd = self::$nsqInstances[$type][$pipe][$scope];
        }
        else
        {
            $nsqd = new nsqphp(null, null, null, self::getLoggerInstance());

            $nsqd->setProxy(self::touchProxyInstance($pipe));

            $warningSet = self::getConfig()->getGlobalSetting('nsq.monitor.msg-bag');

            if ($warningSet && isset($warningSet['nums']) && isset($warningSet['size']))
            {
                $nsqd->setPubBodyWarning($warningSet['nums'], $warningSet['size']);
            }

            self::$nsqInstances[$type][$pipe][$scope] = $nsqd;
        }
        return $nsqd;
    }

    /**
     * @param $pipe
     * @param $config
     * @param $scene
     * @return LookupInterface
     * @throws ServiceInitializationException
     */
    private static function touchLookupdInstance($pipe, $config, $scene = 'mix')
    {
        $scope = $config['scope'];

        // make lookupd instance isolated because DCC will take dynamic results
        $isolated = $config['name'];

        if (isset(self::$lookupInstances[$pipe][$scope][$isolated][$scene]))
        {
            $splitter = self::$lookupInstances[$pipe][$scope][$isolated][$scene];
        }
        else
        {
            $splitter = new IOSplitter();

            if (
                $splitter->registerLogger(self::getLoggerInstance())
                &&
                $splitter->registerProxy(self::touchProxyInstance($pipe))
                &&
                $splitter->registerLookupd(Router::getInstance()->fetchGlobalLookups($config['name'], $scene))
            )
            {
                self::$lookupInstances[$pipe][$scope][$isolated][$scene] = $splitter;
            }
            else
            {
                throw new ServiceInitializationException('IOSplitter register failed');
            }
        }
        return $splitter;
    }

    /**
     * @param $pipe
     * @return Proxy
     */
    private static function touchProxyInstance($pipe)
    {
        if (isset(self::$proxyInstances[$pipe]))
        {
            $proxy = self::$proxyInstances[$pipe];
        }
        else
        {
            $viaHost = $viaPort = null;

            if ($pipe == 'via-proxy')
            {
                $viaHost = self::getConfig()->getGlobalSetting('nsq.proxy.host');
                $viaPort = self::getConfig()->getGlobalSetting('nsq.proxy.port');
            }

            $proxy = new Proxy($viaHost, $viaPort);

            $proxy->setLogger(self::getLoggerInstance());

            self::$proxyInstances[$pipe] = $proxy;
        }
        return $proxy;
    }
}