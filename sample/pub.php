<?php
require 'init.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;


for ($i = 1;;$i++) {
    $s = 'hello 中文' . $i;
    $message = new Message($s);
    $message->setExtends('KEY', 'VALUE');
    //$message->setTag('TAG');
    //if (!Queue::push($topic, [$message, $message])) {
    if (!Queue::push($topic, $message)) {
    //if (!Queue::bulkPush($topic, [$message])) {
        echo "Failed!!!: ".Queue::lastPushError();
        break;
    }
    echo "$s\n";
    usleep(1000 * 100);
}

