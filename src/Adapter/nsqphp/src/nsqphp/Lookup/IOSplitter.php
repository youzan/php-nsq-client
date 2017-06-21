<?php
/**
 * IO Splitter (Sub/Pub)
 * User: moyo
 * Date: 1/14/16
 * Time: 11:59 AM
 */

namespace nsqphp\Lookup;

use nsqphp\Connection\Proxy;
use nsqphp\Exception\LookupException;
use nsqphp\Logger\LoggerInterface;

class IOSplitter implements LookupInterface
{
    /**
     * @var LoggerInterface
     */
    private $provideLogger = null;

    /**
     * @var Proxy
     */
    private $transferProxyBridge = null;

    /**
     * @var Nsqlookupd
     */
    private $subLookupdInstance = [];

    /**
     * @var Nsqlookupd
     */
    private $pubLookupdInstance = [];

    /**
     * @param LoggerInterface $logger
     * @return bool
     */
    public function registerLogger(LoggerInterface $logger)
    {
        $this->provideLogger = $logger;
        return true;
    }

    /**
     * @param $proxyBridge
     * @return bool
     */
    public function registerProxy($proxyBridge)
    {
        $this->transferProxyBridge = $proxyBridge;
        return true;
    }

    /**
     * @param $lookupPool
     * @return bool
     */
    public function registerLookupd($lookupPool)
    {
        $channelM = ['r' => &$this->subLookupdInstance, 'w' => &$this->pubLookupdInstance];
        foreach ($channelM as $type => &$lookupdInstance)
        {
            if (isset($lookupPool[$type]))
            {
                $lookupdNodes = [];
                foreach ($lookupPool[$type] as $lookupdName => $lookupdDSNs)
                {
                    foreach ($lookupdDSNs as $lookupdDSN)
                    {
                        $DSNParsed = parse_url($lookupdDSN);
                        if ($DSNParsed['scheme'] == 'http')
                        {
                            $lookupdNodes[] = $DSNParsed['host'].':'.$DSNParsed['port'];
                        }
                    }
                }
                if ($lookupdNodes)
                {
                    $lookupd = new Nsqlookupd($lookupdNodes);
                    if ($this->provideLogger)
                    {
                        $lookupd->setLogger($this->provideLogger);
                    }
                    if ($this->transferProxyBridge)
                    {
                        $lookupd->setProxy($this->transferProxyBridge);
                    }
                    $lookupdInstance = $lookupd;
                }
            }
        }
        return true;
    }

    /**
     * @param null $topic
     * @param null $sp
     * @return array
     * @throws LookupException
     */
    public function lookupHosts($topic = null, $sp = null)
    {
        switch ($sp)
        {
            case 'pub':
                if ($this->pubLookupdInstance)
                {
                    return $this->pubLookupdInstance->lookupHosts($topic, $sp);
                }
                break;
            case 'sub':
                if ($this->subLookupdInstance)
                {
                    return $this->subLookupdInstance->lookupHosts($topic, $sp);
                }
                break;
        }
        throw new LookupException('lookupd io-splitter failed');
    }
}
