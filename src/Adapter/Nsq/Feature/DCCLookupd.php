<?php
/**
 * DCC lookupd
 * User: moyo
 * Date: 21/12/2016
 * Time: 3:39 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq\Feature;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class DCCLookupd
{
    use SingleInstance;

    public function parsing($clusterName, $bootParsed)
    {
        if (isset($bootParsed['scheme']) && $bootParsed['scheme'] == 'dcc')
        {
            $parameters = [];
            parse_str($bootParsed['query'], $parameters);

            list($app, $module) = explode('~', $parameters['query']);

            $clusterSIGN = '';
            $cSepPos = strpos($clusterName, '-');
            if (is_numeric($cSepPos))
            {
                $clusterSIGN = '_'.substr($clusterName, $cSepPos + 1);
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
    }
}