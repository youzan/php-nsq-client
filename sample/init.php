<?php

define('RESOURCE_PATH', __DIR__.'/resources/');
define('CONFIG_PATH', __DIR__.'/resources/config/');
$config = [
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

$setting = [
//http://10.9.84.100:4161
//    'nsq.server.lookupd.global' => 'http://10.9.84.100:4171',
    //'nsq.server.lookupd.global' => 'http://10.9.84.100:4161',
//    'nsq.server.lookupd.global' => 'http://10.9.152.26:4161',
    'nsq.server.lookupd.global' => 'http://sqs-qa.s.qima-inc.com:4161',
    //'nsq.server.lookupd.global' => 'http://qabb-qa-nsqtest0:4161',
    //'http://nsq-dev.s.qima-inc.com:4161',
//'http://10.9.152.26:4161',
    //'nsq.server.lookupd.global' => 'http://10.9.152.26:4161',
    'nsq.monitor.msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]
];

require __DIR__.'/../vendor/autoload.php';
require __DIR__.'/svc.php';
//require __DIR__.'/Config.php';
class_alias('Kdt\Iron\Queue\Tests\classes\Config', 'Config');
class_alias('Kdt\Iron\Queue\Tests\classes\Log\Log', 'Kdt\Iron\Log\Log');
//class_alias('Kdt\Iron\Queue\Tests\classes\Live\DCC', 'Kdt\Iron\Config\Live\DCC');
//class_alias('Kdt\Iron\Queue\Tests\classes\nsqphp\HTTP', 'nsqphp\Connection\HTTP');

//\Config::init();
use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
use Kdt\Iron\Queue\Adapter\Nsq\Config;

use Kdt\Iron\Queue\Adapter\Nsq\ServiceChain;

//$topic = 'test';
//$topic = 'ordered';
$topic = 'testext';
$channel = 'default';
//$message1 = ['hello', 'world'];
//Config::getInstance()->setGlobalSetting($setting);
//\Config::set($setting);
//Config::getInstance()->addTopicConfig($topic, $config);


