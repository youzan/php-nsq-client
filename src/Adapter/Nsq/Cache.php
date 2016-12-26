<?php
/**
 * Cache via memory
 * User: moyo
 * Date: 20/12/2016
 * Time: 5:59 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Cache\Contract\Protocol\Memory;
use Kdt\Iron\Cache\Foundation\Options;
use Kdt\Iron\Cache\Provider\Memory as Provider;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class Cache
{
    use SingleInstance;

    /**
     * @var Memory
     */
    private $mcInstance = null;

    /**
     * @var string
     */
    private $mcApp = '#kdt-iron-queue';

    /**
     * @var string
     */
    private $mcModule = 'nsq-client';

    /**
     * @var array
     */
    private $history = [];

    /**
     * Cache constructor.
     */
    public function __construct()
    {
        $this->mcInstance = Provider::getInstance($this->mcApp, $this->mcModule, Options::MEMORY_ALLOW_LEGACY);
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

        return $this->mcInstance->host($key, $callback, $ttl);
    }
}