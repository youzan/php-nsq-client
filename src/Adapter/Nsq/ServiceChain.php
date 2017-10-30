<?php
namespace Kdt\Iron\Queue\Adapter\Nsq;

class ServiceChain extends \ZanPHP\Component\ServiceChain\ServiceChain {
    public static function getAll()
    {
        $json = static::get(false);
        return json_decode($json, true) ?: [];
    }
    
    public static function setAll($all)
    {
        $json = json_encode($all);
        if (PHP_SAPI === 'cli') {
            $chain = putenv(static::ENV_KEY.'='.$json);
        } else {
            $key = "HTTP_" . strtoupper(str_replace('-', '_', static::HDR_KEY));
            $_SERVER[$key] = $json;
        }
    }

    public static function clear()
    {
        if (PHP_SAPI === 'cli') {
            putenv(static::ENV_KEY.'='.null);
        } else {
            $key = "HTTP_" . strtoupper(str_replace('-', '_', static::HDR_KEY));
            unset($_SERVER[$key]);
        }
    }
}
