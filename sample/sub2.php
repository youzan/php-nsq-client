<?php
require 'init.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;
$result = Queue::pop([$topic, $channel], function($message) {
    printf("[%s](%s): %s\n", $message->getId(), $message->getTag(), $message->getPayload());
    //var_dump($message);
    //Queue::exitPop();
    /*
    for ($i = 0; $i < 7; $i++) {
        sleep(10);
        echo ".";
        Queue::ping();
        echo ".\n";
    }*/
    Queue::delete($message->getId());
    printf("ack %s\n", $message->getId());
}, ['auto_delete'=>false, 'msg_timeout'=>100000, 'keep_seconds'=>5]);//, 'tag'=>'TAG']);

echo "exit!\n";
var_dump($result);

