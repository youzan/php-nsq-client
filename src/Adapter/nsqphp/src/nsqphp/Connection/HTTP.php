<?php
/**
 * Simple HTTP Client
 * User: moyo
 * Date: 24/12/2016
 * Time: 4:14 PM
 */

namespace nsqphp\Connection;

use nsqphp\nsqphp;

class HTTP
{
    /**
     * @var string
     */
    private static $agent = 'nsqphp/'.nsqphp::VERSION;

    /**
     * @var array
     */
    private static $headers = ['Accept: application/vnd.nsq; version=1.0'];

    /**
     * @var string
     */
    private static $encoding = '';

    /**
     * @param $url
     * @param array $extOptions
     * @return array
     */
    public static function get($url, $extOptions = [])
    {
        return self::request($url, [], $extOptions);
    }

    /**
     * @param $url
     * @param $data
     * @param array $extOptions
     * @return array
     */
    public static function post($url, $data, $extOptions = [])
    {
        return self::request($url, [CURLOPT_POST => TRUE, CURLOPT_POSTFIELDS => $data], $extOptions);
    }

    /**
     * @param $url
     * @param $selfOptions
     * @param $usrOptions
     * @return array
     */
    private static function request($url, $selfOptions, $usrOptions)
    {
        $ch = curl_init();

        $initOptions = [
            CURLOPT_URL            => $url,
            CURLOPT_RETURNTRANSFER => TRUE,
            CURLOPT_HEADER         => FALSE,
            CURLOPT_FOLLOWLOCATION => FALSE,
            CURLOPT_ENCODING       => self::$encoding,
            CURLOPT_USERAGENT      => self::$agent,
            CURLOPT_HTTPHEADER     => self::$headers,
            CURLOPT_FAILONERROR    => TRUE
        ];

        $selfOptions && $initOptions = self::mergeOptions($initOptions, $selfOptions);
        $usrOptions  && $initOptions = self::mergeOptions($initOptions,  $usrOptions);

        curl_setopt_array($ch, $initOptions);

        $result = curl_exec($ch);

        $error = curl_errno($ch) ? [curl_errno($ch), curl_error($ch)] : null;

        curl_close($ch);

        return [$error, $result];
    }

    /**
     * @param $base
     * @param $custom
     * @return mixed
     */
    private static function mergeOptions($base, $custom)
    {
        foreach ($custom as $key => $val)
        {
            $base[$key] = $val;
        }
        return $base;
    }
}