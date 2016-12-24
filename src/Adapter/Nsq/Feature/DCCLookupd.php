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
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class DCCLookupd
{
    use SingleInstance;

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
            $clientRole = $usingScene == 'pub' ? 'producer' : 'consumer';

            $cloudStrategy = DCC::gets([sprintf($app, $topicConfig['group']), sprintf($module, $clientRole)], [$defaultKey, $topicKey]);

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
                                $this->append($foundNodes, $producerTargets['current']);
                            }
                            else
                            {
                                $this->append($foundNodes, $producerTargets['previous']);
                            }
                        }
                        else
                        {
                            $this->append($foundNodes, $producerTargets['previous']);
                        }
                    }
                    else
                    {
                        if (isset($producerTargets['current']))
                        {
                            $this->append($foundNodes, $producerTargets['current']);
                        }
                        else if (isset($producerTargets['previous']))
                        {
                            $this->append($foundNodes, $producerTargets['previous']);
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
                }
            }
        }

        return $foundNodes;
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