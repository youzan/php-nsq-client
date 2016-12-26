<?php
/**
 * test for sharding
 * User: moyo
 * Date: 22/12/2016
 * Time: 5:58 PM
 */

return [
    'lookupd_pool' => [
        'lookupd-default'
    ],
    'rw_strategy' => [
        'sharding_partition_missing' => ['lookupd-default' => '--', 'lookupd-sp-missing' => 'rw'],
        'sharding_partition_empty'   => ['lookupd-default' => '--', 'lookupd-sp-empty'   => 'rw'],
    ],
    'sharding_enabled' => [
        'sharding_with_enabled' => true,
        'sharding_partition_missing' => true,
        'sharding_partition_empty' => true,
    ],
    'topic' => [
        'sharding_topic_normal' => 'sharding_topic_normal',
        'sharding_with_enabled' => 'sharding_with_enabled',
        'sharding_partition_missing' => 'sharding_partition_missing',
        'sharding_partition_empty' => 'sharding_partition_empty',
    ]
];