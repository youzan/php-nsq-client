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
    ],
    'topic' => [
        'dsn_topic_syntax_old' => 'dsn_topic_syntax_old',
        'dsn_topic_normal' => 'dsn_topic_normal',
        'dsn_topic_balanced' => 'dsn_topic_balanced',
    ]
];