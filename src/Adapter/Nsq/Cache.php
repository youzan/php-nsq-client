<?php
/**
 * Cache via memory
 * User: moyo
 * Date: 20/12/2016
 * Time: 5:59 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class Cache
{
    use SingleInstance;

    /**
     * @var Memory
     */
    private $m = [];

    /**
     * @var string
     */
    private $prefix = '#nsq-client-';

    /**
     * @var array
     */
    private $history = [];


    private function hasApcu()
    {
        static $ret;
        if (!isset($ret))
        {
            $ret = extension_loaded('apcu') && function_exists('apcu_enabled') && apcu_enabled();
        }
        return $ret;
    }

    /**
     * Cache constructor.
     */
    public function __construct()
    {
    }

    /**
     * @return void
     */
    public function clear()
    {
        foreach ($this->history as $idx => $type)
        {
            if ($type == 'host' && $this->mcInstance->del($idx))
            {
                unset($this->history[$idx]);
            }
        }
    }

    /**
     * @param $key
     * @param callable $callback
     * @param $ttl
     * @return array
     */
    public function host($key, callable $callback, $ttl)
    {
        $this->history[$key] = 'host';
        $ret = $this->get($key);
        if ($ret !== null)
        {
            return $ret;
        }
        $ret = call_user_func($callback);
        if (is_null($ret))
        {
            return null;
        }
        $this->set($key, $ret, $ttl);
        return $ret;
    }

    private function get($key)
    {
        if ($this->hasApcu())
        {
            $ok = false;
            $result = apcu_fetch($this->prefix . $key, $ok);
            if ($ok)
            {
                return $result;
            }
        }
        else
        {
            return isset($this->m[$key]) ? $this->m[$key] : null;
        }
    }
   
    private function del($key)
    {
        if ($this->hasApcu())
        {
            return apcu_delete($this->prefix . $key);
        }
        else
        {
            unset($this->m[$key]);
        }
    }

    private function set($key, $value, $ttl)
    {
        if ($this->hasApcu())
        {
            return apcu_store($this->prefix . $key, $value, $ttl);
        }
        else
        {
            $this->m[$key] = $value;
        }
    }
}

