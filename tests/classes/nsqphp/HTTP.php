<?php
/**
 * nsqphp HTTP mock
 * User: moyo
 * Date: 24/12/2016
 * Time: 4:51 PM
 */

namespace Kdt\Iron\Queue\Tests\classes\nsqphp;

class HTTP
{
    /**
     * @var array
     */
    private static $data = [];

    /**
     * @var int
     */
    private static $reqCount = 0;

    /**
     * @param $url
     * @param array $options
     * @return array
     */
    public static function get($url, $options = [])
    {
        return self::requestLocal($url);
    }

    /**
     * @param $url
     * @param $data
     * @param array $options
     * @return array
     */
    public static function post($url, $data, $options = [])
    {
        return self::requestLocal($url, $data);
    }

    /**
     * @return int
     */
    public static function reqCount()
    {
        return self::$reqCount;
    }

    /**
     * @param $targetURL
     * @param array $expectSubmit
     * @return mixed
     * @throws \Exception
     */
    private static function requestLocal($targetURL, $expectSubmit = [])
    {
        self::$reqCount ++;

        $parsed = parse_url($targetURL);
        $template = self::loadMocks($parsed['host'], $parsed['port']);

        $uri = $parsed['path'].(isset($parsed['query'])?('?'.$parsed['query']):'');

        if (isset($template[$uri]))
        {
            $request = $template[$uri]['request'];
            $response = $template[$uri]['response'];

            if ($request)
            {
                if (is_string($request))
                {
                    if (substr($request, 0, 5) == '[BIN]')
                    {
                        $request = hex2bin(substr($request, 5));
                    }
                }

                if ($request != $expectSubmit)
                {
                    return [[1, "Request data wrongs <{$expectSubmit}|{$request}>"], null];
                }
            }

            if ($response)
            {
                return [null, json_encode($response)];
            }
            else
            {
                return [[1, "Non response defined"], null];
            }
        }
        else
        {
            throw new \Exception('Net-mock URI missing ~ '.$targetURL);
        }
    }

    /**
     * @param $host
     * @param $port
     * @return mixed
     * @throws \Exception
     */
    private static function loadMocks($host, $port)
    {
        if (isset(self::$data[$host][$port]))
        {
            return self::$data[$host][$port];
        }
        else
        {
            $file = RESOURCE_PATH.'net-mocks/'.str_replace('.', '_', $host).'-'.$port.'.json';
            if (is_file($file))
            {
                $template = json_decode(file_get_contents($file), true);
                if (is_array($template))
                {
                    self::$data[$host][$port] = $template;
                    return $template;
                }
                else
                {
                    throw new \Exception('Net-mock data invalid ~ '.$file);
                }
            }
            else
            {
                throw new \Exception('Net-mock data not found ~ '.$file);
            }
        }
    }
}