<?php
require 'init.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
use Kdt\Iron\Queue\Adapter\Nsq\ServiceChain;

$result = Queue::pop([$topic, $channel], function($message) {
    printf("[%s]%s: %s\n", $message->getId(), $message->getTag(), var_export($message->getPayload(), true));
    //var_dump(ServiceChain::getAll());    
    //var_dump($message);
    //Queue::exitPop();
    echo $message->getExtends('KEY'), "\n";
    /*
    for ($i = 0; $i < 7; $i++) {
        sleep(10);
        echo ".";
        Queue::ping();
        echo ".\n";
    }*/
    Queue::delete($message->getId());
    printf("ack %s\n", $message->getId());
//}, ['auto_delete'=>false, 'msg_timeout'=>100000, 'tag'=>'TAG']);
//}, ['auto_delete'=>false, 'sub_ordered'=>true, 'ext_filter'=>['KEY','VALUE']]);
}, ['auto_delete'=>false, 'sub_ordered'=>true]);
//}, ['auto_delete'=>false, 'msg_timeout'=>100000]);
echo "exit!\n";
var_dump($result);

