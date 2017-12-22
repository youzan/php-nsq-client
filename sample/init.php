<?php

$config = [
    'lookupd_pool' => [
        'global',
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

$setting = [
    'nsq.server.lookupd.global' => 'http://sqs-qa.s.qima-inc.com:4161',
    'nsq.monitor.msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]
];

require __DIR__.'/../vendor/autoload.php';
require __DIR__.'/svc.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
use Kdt\Iron\Queue\Adapter\Nsq\Config;

//$topic = 'test';
//$topic = 'ordered';
$topic = 'testext';
$channel = 'default';
//$message1 = ['hello', 'world'];
Config::getInstance()->setGlobalSetting($setting);
//\Config::set($setting);
Config::getInstance()->addTopicConfig($topic, $config);


