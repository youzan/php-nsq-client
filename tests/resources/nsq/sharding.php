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
    'sharding_enabled' => [
        'sharding_with_enabled' => true,
    ],
    'topic' => [
        'sharding_topic_normal' => 'sharding_topic_normal',
        'sharding_with_enabled' => 'sharding_with_enabled',
    ]
];