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

$topic = 'test';
$channel = 'test';
$message0 = 'hello world';
$message1 = ['hello', 'world'];

Config::getInstance()->setGlobalSetting($setting);
Config::getInstance()->addTopicConfig($topic, $config);

var_dump(Queue::push($topic, $message0));
var_dump(Queue::push($topic, $message1));

var_dump(Queue::pop([$topic, $channel], function($message) {
    var_dump($message);
    //Queue::exitPop();
}));

