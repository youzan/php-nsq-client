<?php
/**
 * env
 * User: moyo
 * Date: 8/10/16
 * Time: 5:02 PM
 */

// test env

define('RESOURCE_PATH', realpath(__DIR__ . '/../') . '/resources/');

// class aliases

class_alias('Kdt\Iron\Queue\Tests\classes\Config', 'Config');
class_alias('Kdt\Iron\Queue\Tests\classes\Log\Log', 'Kdt\Iron\Log\Log');
class_alias('Kdt\Iron\Queue\Tests\classes\nsqphp\HTTP', 'nsqphp\Connection\HTTP');
