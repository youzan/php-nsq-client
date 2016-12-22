<?php
/**
 * Config mock
 * User: moyo
 * Date: 8/10/16
 * Time: 4:48 PM
 */

namespace Kdt\Iron\Queue\Tests\classes;

class Config
{
    /**
     * @var array
     */
    private static $configs = [];

    /**
     * @param $name
     *
     * @return mixed|null
     */
    public static function get($name)
    {
        $config = self::loading($name);

        return isset($config[$name]) ? $config[$name] : null;
    }

    /**
     * @param $fullKey
     * @return mixed
     */
    private static function loading($fullKey)
    {
        $fileName = substr($fullKey, 0, strpos($fullKey, '.'));
        if (!isset(self::$configs[$fileName]))
        {
            self::$configs[$fileName] = require RESOURCE_PATH . 'config/'.$fileName.'.php';
        }
        return self::$configs[$fileName];
    }
}