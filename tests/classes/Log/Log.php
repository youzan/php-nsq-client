<?php
/**
 * KDT logger
 * User: moyo
 * Date: 22/12/2016
 * Time: 6:08 PM
 */

namespace Kdt\Iron\Queue\Tests\classes\Log;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class Log
{
    use SingleInstance;

    public function __call($name, $arguments)
    {
        // perhaps is error, warn, info, debug
        // ignore
    }
}