<?php
/**
 * nsq config
 * User: moyo
 * Date: 22/12/2016
 * Time: 6:01 PM
 */

return [
    'nsq.testing.config.get' => 'hi',

    'nsq.server.lookupd.lookupd-default' => ['http://127.0.0.1:2'],
    'nsq.server.lookupd.lookupd-router' => ['http://127.0.0.1:3'],
    'nsq.server.lookupd.lookupd-dsn-syntax-old' => ['http:127.0.0.1:2'],
    'nsq.server.lookupd.lookupd-dsn-normal' => ['http://127.0.0.1:2'],
    'nsq.server.lookupd.lookupd-dsn-balanced' => ['http://127.0.0.1:2', 'http://127.0.0.1:3'],
    'nsq.server.lookupd.lookupd-dsn-discovery' => ['http://127.0.0.2:1'],

    'nsq.server.lookupd.lookupd-sp-missing' => ['http://127.0.0.2:8'],
    'nsq.server.lookupd.lookupd-sp-empty' => ['http://127.0.0.2:8'],

    'nsq.server.lookupd.global-dcc-default' => ['dcc://local/parameters?query=%s.nsq.lookupd.addr~%s&fallback=http://127.0.0.3:12345'],
    'nsq.server.lookupd.global-dcc-before' => ['dcc://local/parameters?query=%s.nsq.lookupd.addr~%s&fallback=http://127.0.0.3:12345'],
    'nsq.server.lookupd.global-dcc-moving' => ['dcc://local/parameters?query=%s.nsq.lookupd.addr~%s&fallback=http://127.0.0.3:12345'],
    'nsq.server.lookupd.global-dcc-finish' => ['dcc://local/parameters?query=%s.nsq.lookupd.addr~%s&fallback=http://127.0.0.3:12345'],
];