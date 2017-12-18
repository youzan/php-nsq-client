<?php
require 'init.php';

use Kdt\Iron\Queue\Queue;
use Kdt\Iron\Queue\Message;

use Kdt\Iron\Queue\Adapter\Nsq\ServiceChain;
$svc = ServiceChain::getAll();
$svc['zan_test'] = true;
ServiceChain::setAll($svc);


for ($i = 1;;$i++) {
    $s = 'hello 中文' . $i;
    $message = new Message($s);
    //$message->setTag('TAG');
    //if (!Queue::push($topic, [$message, $message])) {
    if (!Queue::bulkPush($topic, [$message])) {
        echo "Failed!!!: ".Queue::lastPushError();
        break;
    }
    echo "$s\n";
    usleep(1000 * 100);
}

