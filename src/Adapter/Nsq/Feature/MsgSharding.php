<?php
/**
 * Message sharding
 * User: moyo
 * Date: 21/12/2016
 * Time: 5:46 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq\Feature;

use Kdt\Iron\Queue\Adapter\Nsq\Router;
use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

use Kdt\Iron\Queue\Message as QMessage;
use nsqphp\Message\Message as NSQMessage;

use nsqphp\Exception\ShardingStrategyException;

class MsgSharding
{
    use SingleInstance;

    /**
     * @param $topic
     * @param QMessage $origin
     * @param NSQMessage $target
     */
    public function process($topic, QMessage $origin, NSQMessage $target)
    {
        $proof = $origin->getShardingProof();
        if ($proof)
        {
            $pubNodes = Router::getInstance()->fetchPublishNodes($topic);

            $mapPrefix = 'pt_';
            $knownPartitionNum = null;
            $partitionNodes = [];

            foreach ($pubNodes as $node)
            {
                if (isset($node['meta']))
                {
                    $partitionNum = isset($node['meta']['partitions']) ? $node['meta']['partitions'] : -1;
                    if ($partitionNum > 0)
                    {
                        if (is_null($knownPartitionNum))
                        {
                            $knownPartitionNum = $partitionNum;
                        }
                        else if ($knownPartitionNum != $partitionNum)
                        {
                            throw new ShardingStrategyException('Conflict partitions info');
                        }

                        $partitionNodes[$mapPrefix.$node['partition']] = $node;
                    }
                }
            }

            if ($knownPartitionNum && $partitionNodes)
            {
                $limitedSlot = $mapPrefix.(string)($proof % $knownPartitionNum);
                if (isset($partitionNodes[$limitedSlot]))
                {
                    $target->setLimitedNode($partitionNodes[$limitedSlot]);
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
}