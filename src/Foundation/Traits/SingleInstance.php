<?php
/**
 * Single instance
 * User: moyo
 * Date: 20/12/2016
 * Time: 3:13 PM
 */

namespace Kdt\Iron\Queue\Foundation\Traits;

trait SingleInstance
{
    /**
     * @var static
     */
    private static $instanceObject = null;

    /**
     * @return static
     */
    public static function getInstance()
    {
        if (is_null(self::$instanceObject))
        {
            self::$instanceObject = new static;
        }
        return self::$instanceObject;
    }
}