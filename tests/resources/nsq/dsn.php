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
        'dsn_topic_dcc_before' => ['global-dcc-before' => 'rw'],
        'dsn_topic_dcc_moving' => ['global-dcc-moving' => 'rw'],
        'dsn_topic_dcc_finish' => ['global-dcc-finish' => 'rw'],
    ],
    'topic' => [
        'dsn_topic_syntax_old' => 'dsn_topic_syntax_old',
        'dsn_topic_normal' => 'dsn_topic_normal',
        'dsn_topic_balanced' => 'dsn_topic_balanced',
        'dsn_topic_discovery' => 'dsn_topic_discovery',
        'dsn_topic_dcc_before' => 'dsn_topic_dcc_before',
        'dsn_topic_dcc_moving' => 'dsn_topic_dcc_moving',
        'dsn_topic_dcc_finish' => 'dsn_topic_dcc_finish',
    ]
];