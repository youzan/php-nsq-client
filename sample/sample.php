<?php

$config = [
    'lookupd_pool' => [
        'global'
    ],

    'rw_strategy' => [
        'test' => ['global' => 'rw'],
    ],

    'topic' => [
        'test' => 'test_bench',
    ]
];

$setting = [
    'nsq.server.lookupd.global' => 'http://127.0.0.1:4161',
    'nsq.monitor.msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]
];

require __DIR__.'/../vendor/autoload.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
use Kdt\Iron\Queue\Adapter\Nsq\Config;

$loopup = 'http://10.9.80.209:4161';
$topic = 'test';
$message = 'hello world';
Config::getInstance()->setGlobalSetting($setting);
Config::getInstance()->addTopicConfig('test', $config);

var_dump(Queue::push($topic, $message));

