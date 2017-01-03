<?php
/**
 * test for dsn
 * User: moyo
 * Date: 23/12/2016
 * Time: 4:27 PM
 */

return [
    'lookupd_pool' => [],
    'rw_strategy' => [
        'dsn_topic_syntax_old' => ['lookupd-dsn-syntax-old' => 'rw'],
        'dsn_topic_normal' => ['lookupd-dsn-normal' => 'rw'],
        'dsn_topic_balanced' => ['lookupd-dsn-balanced' => 'rw'],
        'dsn_topic_discovery' => ['lookupd-dsn-discovery' => 'rw'],
        'dsn_topic_discovery_non' => ['lookupd-dsn-discovery-non' => 'rw'],

        'dsn_topic_dcc_default' => ['global-dcc-default' => 'rw'],
        'dsn_topic_dcc_before' => ['global-dcc-before' => 'rw'],
        'dsn_topic_dcc_moving_gray_miss' => ['global-dcc-moving' => 'rw'],
        'dsn_topic_dcc_moving_gray_hit' => ['global-dcc-moving' => 'rw'],
        'dsn_topic_dcc_moving_non' => ['global-dcc-moving' => 'rw'],
        'dsn_topic_dcc_finish' => ['global-dcc-finish' => 'rw'],
    ],
    'topic' => [
        'dsn_topic_syntax_old' => 'dsn_topic_syntax_old',
        'dsn_topic_normal' => 'dsn_topic_normal',
        'dsn_topic_balanced' => 'dsn_topic_balanced',
        'dsn_topic_discovery' => 'dsn_topic_discovery',
        'dsn_topic_discovery_non' => 'dsn_topic_discovery_non',

        'dsn_topic_dcc_default' => 'dsn_topic_dcc_default',
        'dsn_topic_dcc_before' => 'dsn_topic_dcc_before',
        'dsn_topic_dcc_moving_gray_miss' => 'dsn_topic_dcc_moving_gray_miss',
        'dsn_topic_dcc_moving_gray_hit' => 'dsn_topic_dcc_moving_gray_hit',
        'dsn_topic_dcc_moving_non' => 'dsn_topic_dcc_moving_non',
        'dsn_topic_dcc_finish' => 'dsn_topic_dcc_finish',
    ]
];