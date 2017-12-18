<?php
return [
    //'server' => ["lookupd" => ['global' => ['http://sqs-qa.s.qima-inc.com:4161']]],
    'nsq.server.lookupd.global' => ['http://sqs-qa.s.qima-inc.com:4161'],
    //'nsq.server.lookupd.global' => 'http://qabb-qa-nsqtest0:4161',
    //'monitor' => ['msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]],
    'nsq.monitor.msg-bag' => [ 'nums' => 1000, 'size' => 65536 ]
];
