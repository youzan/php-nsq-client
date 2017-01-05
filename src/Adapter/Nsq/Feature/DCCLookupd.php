<?php
/**
 * DCC lookupd
 * User: moyo
 * Date: 21/12/2016
 * Time: 3:39 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq\Feature;

use Kdt\Iron\Config\Live\DCC;
use Kdt\Iron\Queue\Adapter\Nsq\Config;
use Kdt\Iron\Queue\Adapter\Nsq\InstanceMgr;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Exception as SysException;

class DCCLookupd
{
    use SingleInstance;

    /**
     * @var string
     */
    private $specialGroupPrefix = 'binlog';

    /**
     * @param $clusterName
     * @param $bootParsed
     * @param $topicNamed
     * @param $usingScene
     * @return array
     */
    public function parsing($clusterName, $bootParsed, $topicNamed, $usingScene)
    {
        $foundNodes = [];

        if (isset($bootParsed['scheme']) && $bootParsed['scheme'] == 'dcc')
        {
            $topicConfig = Config::getInstance()->getTopicConfig($topicNamed);

            // get AM info
            $parameters = [];
            parse_str($bootParsed['query'], $parameters);

            list($app, $module) = explode('~', $parameters['query']);

            // get cluster sign like "sync", "sqs"
            $clusterSIGN = '';
            $cSepPos = strpos($clusterName, '-');
            if (is_numeric($cSepPos))
            {
                $clusterSIGN = '_'.substr($clusterName, $cSepPos + 1);
            }

            // DCC related keys
            $defaultKey = '##_default'.$clusterSIGN;
            $topicKey = $topicConfig['topic'];
            $groupKey = $this->getGroupKey($topicKey);
            $clientRole = $usingScene == 'pub' ? 'producer' : 'consumer';

            try
            {
                $cloudStrategy = DCC::gets([sprintf($app, $groupKey), sprintf($module, $clientRole)], [$defaultKey, $topicKey]);
            }
            catch (SysException $e)
            {
                $cloudStrategy = [];
            }

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
                        $this->append($foundNodes, $usedStrategy['previous']);
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
                        $this->append($foundNodes, $usedStrategy['current']);
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

                        $grayRule = null;
                        $grayHit = false;

                        if (isset($grayHosts[$localHost]))
                        {
                            $grayRule = $grayHosts[$localHost];
                        }
                        else if (isset($grayHosts['*']))
                        {
                            $grayRule = $grayHosts['*'];
                        }

                        if ($grayRule)
                        {
                            if (isset($grayRule['percent']))
                            {
                                $grayPercentK = round($grayRule['percent'], 3) * 1000;
                                if ($grayPercentK >= rand(0, 100000))
                                {
                                    $grayHit = true;
                                }
                            }
                        }

                        if ($grayHit)
                        {
                            $this->append($foundNodes, $producerTargets['current']);
                        }
                        else
                        {
                            $this->append($foundNodes, $producerTargets['previous']);
                        }
                    }
                    else
                    {
                        if (isset($producerTargets['previous']))
                        {
                            $this->append($foundNodes, $producerTargets['previous']);
                        }
                        else if (isset($producerTargets['current']))
                        {
                            $this->append($foundNodes, $producerTargets['current']);
                        }
                    }
                }
            }

            if (empty($foundNodes))
            {
                // -> fallback
                if (isset($parameters['fallback']))
                {
                    $foundNodes = [$parameters['fallback']];
                    InstanceMgr::getLoggerInstance()->info('DCC has fallback to '.$parameters['fallback']);
                }
            }
            else
            {
                InstanceMgr::getLoggerInstance()->debug('Found lookupd nodes via DCC ~ '.$clusterName.'/'.$topicNamed);
            }
        }

        return $foundNodes;
    }

    /**
     * @param $topicParsed
     * @return string
     */
    private function getGroupKey($topicParsed)
    {
        $groupL1Pos = strpos($topicParsed, '_');
        if ($groupL1Pos)
        {
            $groupL1Val = substr($topicParsed, 0, $groupL1Pos);
            if ($groupL1Val == $this->specialGroupPrefix)
            {
                $groupL2Pos = strpos($topicParsed, '_', $groupL1Pos + 1);
                if ($groupL2Pos)
                {
                    $groupL2Val = substr($topicParsed, 0, $groupL2Pos);

                    return $groupL2Val;
                }
            }

            return $groupL1Val;
        }

        return $topicParsed;
    }

    /**
     * @param $base
     * @param $more
     */
    private function append(&$base, $more)
    {
        $base = array_merge($base, $more);
    }
}