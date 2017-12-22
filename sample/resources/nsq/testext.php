<?php
return [
    'lookupd_pool' => [
        'global',
        //'global-sqs'

    ],

    'rw_strategy' => [
        'test' => ['global' => 'rw'],
    ],

    'topic' => [
        'test' => 'test_php',
        'testext' => 'test_php_ext',
        'ordered' => 'test_php_ordered',
    ]
];
