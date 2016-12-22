<?php
/**
 * Event callbacks
 * User: moyo
 * Date: 20/12/2016
 * Time: 8:59 PM
 */

namespace Kdt\Iron\Queue\Foundation\Traits;

use Exception as SysException;

trait EventCallback
{
    /**
     * @var array
     */
    private $regEventCallbacks = [];

    /**
     * @param $name
     * @param callable $callback
     */
    public function registerEvent($name, callable $callback)
    {
        $this->regEventCallbacks[$name][] = $callback;
    }

    /**
     * @param $eventName
     * @param SysException $presentException
     */
    protected function triggerEvent($eventName, SysException $presentException = null)
    {
        if (isset($this->regEventCallbacks[$eventName]))
        {
            foreach ($this->regEventCallbacks[$eventName] as $callback)
            {
                if (is_callable($callback))
                {
                    try
                    {
                        call_user_func_array($callback, [$presentException]);
                    }
                    catch (SysException $e) { }
                }
            }
        }
    }
}