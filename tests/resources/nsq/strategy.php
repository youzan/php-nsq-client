<?php
/**
 * test for strategy
 * User: moyo
 * Date: 23/12/2016
 * Time: 2:42 PM
 */

return [
    'lookupd_pool' => [
        'lookupd-c',
    ],
    'rw_strategy' => [
        'strategy_cluster_only_a_0' => ['lookupd-c' => '--', 'lookupd-a' => 'rw'],
        'strategy_cluster_only_b_r' => ['lookupd-c' => '--', 'lookupd-b' => 'r'],
        'strategy_cluster_r_c_r_b'  => ['lookupd-c' => 'r', 'lookupd-b' => 'r'],
        'strategy_cluster_r_a_rw_b' => ['lookupd-c' => '--', 'lookupd-a' => 'r', 'lookupd-b' => 'rw'],
        'strategy_cluster_w_b_rw_a' => ['lookupd-c' => '--', 'lookupd-a' => 'rw', 'lookupd-b' => 'w'],
        'strategy_cluster_rw_c_r_a_w_b' => ['lookupd-a' => 'r', 'lookupd-b' => 'w'],
    ],
    'topic' => [
        'strategy_cluster_default0' => 'strategy_cluster_default0',
        'strategy_cluster_only_a_0' => 'strategy_cluster_only_a_0',
        'strategy_cluster_only_b_r' => 'strategy_cluster_only_b_r',
        'strategy_cluster_r_c_r_b'  => 'strategy_cluster_r_c_r_b',
        'strategy_cluster_r_a_rw_b' => 'strategy_cluster_r_a_rw_b',
        'strategy_cluster_w_b_rw_a' => 'strategy_cluster_w_b_rw_a',
        'strategy_cluster_rw_c_r_a_w_b' => 'strategy_cluster_rw_c_rw_ab'
    ]
];