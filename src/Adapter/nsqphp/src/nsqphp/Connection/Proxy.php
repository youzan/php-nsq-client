<?php

namespace nsqphp\Connection;

use Guzzle\Http\Client;
use Guzzle\Http\Exception\BadResponseException;
use nsqphp\Logger\LoggerInterface;
use nsqphp\Wire\Writer;

class Proxy
{
    /**
     * @var bool
     */
    private $enabled = false;

    /**
     * @var string
     */
    private $host = null;

    /**
     * @var string
     */
    private $port = null;

    /**
     * @var Client
     */
    private $client = null;

    /**
     * @var LoggerInterface
     */
    private $logger = null;

    /**
     * @var Writer
     */
    private $wireWriter = null;

    /**
     * @var float
     */
    private $connectionTimeout = 1.0;

    /**
     * @var float
     */
    private $responseTimeout = 2.0;

    /**
     * stash
     * @var array
     */
    private $proxyPipes = [];

    /**
     * @param $host
     * @param $port
     */
    public function __construct($host = null, $port = null)
    {
        $this->host = $host;
        $this->port = $port;
        if ($this->host && $this->port)
        {
            $this->enabled = true;
        }
    }

    /**
     * @param $logger
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    /**
     * @return bool
     */
    public function isEnabled()
    {
        return $this->enabled;
    }

    /**
     * @return bool
     */
    public function notEnabled()
    {
        return ! $this->enabled;
    }

    /**
     * @param $hosts
     * @param \closure $callback
     * @return array
     */
    public function lookupNodes($hosts, \closure $callback)
    {
        $tries = [];
        if ($this->enabled)
        {
            $lookups = [];
            foreach ($hosts as $lookupd)
            {
                list($host, $port) = explode(':', $lookupd);
                $proxy = $this->fetchProxyConnection('lookup-nodes', $host, $port, 'http');
                $proxy && $lookups[] = $proxy;
            }
            $lookups && $tries[] = $lookups;
        }
        $nodes = [];
        $tries[] = $hosts;
        foreach ($tries as $try)
        {
            $nodes = call_user_func_array($callback, [$try]);
            if ($nodes)
            {
                break;
            }
        }
        return $nodes;
    }

    /**
     * @param $host
     * @param $port
     * @param \closure $callback
     * @return resource
     */
    public function initSocket($host, $port, \closure $callback)
    {
        $tries = [];
        if ($this->enabled)
        {
            $proxy = $this->fetchProxyConnection('init-socket', $host, $port, 'unix', $this->wireWriter()->magic());
            if ($proxy)
            {
                $tries[] = ['host' => 'unix://'.$proxy, 'port' => -1];
            }
            else
            {
                // fail on proxy
                // disable it for nsq protocol (magic identifier)
                $this->enabled = false;
            }
        }
        $socket = false;
        $tries[] = ['host' => $host, 'port' => $port];
        foreach ($tries as $i => $try)
        {
            $socket = call_user_func_array($callback, [$try['host'], $try['port']]);
            if ($socket)
            {
                break;
            }
            else
            {
                if (count($tries) > 1 && $i == 0)
                {
                    // fail on proxy
                    // disable it for nsq protocol (magic identifier)
                    $this->enabled = false;
                }
            }
        }
        return $socket;
    }

    /**
     * @param $host
     * @param $port
     * @param \closure $callback
     * @return string
     * @throws \Exception
     */
    public function httpPost($host, $port, \closure $callback)
    {
        $tries = [];
        if ($this->enabled)
        {
            $proxy = $this->fetchProxyConnection('http-post', $host, $port, 'http');
            $proxy && $tries[] = ['host' => $proxy, 'port' => -1];
        }
        $buffer = '';
        $tries[] = ['host' => $host, 'port' => $port];
        foreach ($tries as $i => $try)
        {
            try
            {
                $buffer = call_user_func_array($callback, [$try['host'], $try['port']]);
                if ($buffer)
                {
                    break;
                }
            }
            catch (\Exception $e)
            {
                if ($i == count($tries) - 1)
                {
                    // throw exception at last trying
                    throw $e;
                }
            }
        }
        return $buffer;
    }

    /**
     * @param $scene
     * @param $host
     * @param $port
     * @param $protocol
     * @param string $magic
     * @return string
     */
    private function fetchProxyConnection($scene, $host, $port, $protocol, $magic = '')
    {
        $sk = implode('/', [$scene, $host, $port, $protocol, $magic]);
        if (isset($this->proxyPipes[$sk]))
        {
            $pipe = $this->proxyPipes[$sk];
        }
        else
        {
            $url = 'http://'.$this->host.':'.$this->port.'/api/get-proxy-conn';
            $data = [
                'Pid' => (string)getmypid(),
                'RemoteAddr' => $host.':'.$port,
                'ConnId' => 0,
                'Protocol' => $protocol == 'http' ? 'HTTP' : 'unix',
                'InitSendData' => $magic
            ];
            try
            {
                $response = $this->makeHttpPost($url, json_encode($data));
                $this->logger->debug('#proxy# found it ('.$scene.') -> ['.$response.']');
            }
            catch (\Exception $e)
            {
                $response = null;
                $this->logger->error('#proxy# touch exception : '.$e->getMessage());
            }
            $this->proxyPipes[$sk] = $pipe = $response;
        }
        return $pipe;
    }

    /**
     * @param $url
     * @param $body
     * @return string
     */
    private function makeHttpPost($url, $body)
    {
        $client = $this->getHttpClient();
        try
        {
            $response = $client->post($url, null, $body)->send();
        }
        catch (BadResponseException $req)
        {
            $response = $req->getResponse();
        }
        $body = $response->getBody();
        $size = $body->getSize();
        if ($body->seek(0))
        {
            $buffer = $body->read($size) ?: '';
        }
        else
        {
            $buffer = (string)$body;
        }
        return $buffer;
    }

    /**
     * @return Client
     */
    private function getHttpClient()
    {
        if (is_null($this->client))
        {
            $this->client = new Client('http://'.$this->host.':'.$this->port, [
                'connect_timeout' => $this->connectionTimeout,
                'timeout' => $this->responseTimeout
            ]);
        }
        return $this->client;
    }

    /**
     * @return Writer
     */
    private function wireWriter()
    {
        if (is_null($this->wireWriter))
        {
            $this->wireWriter = new Writer();
        }
        return $this->wireWriter;
    }
}