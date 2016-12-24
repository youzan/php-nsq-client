<?php
/**
 * DCC mock
 * User: moyo
 * Date: 24/12/2016
 * Time: 11:43 PM
 */

namespace Kdt\Iron\Queue\Tests\classes\Live;

class DCC
{
    /**
     * @var array
     */
    private static $data = [];

    /**
     * @param array $amk
     * @param $default
     * @return mixed
     */
    public static function get(array $amk, $default = null)
    {
        list($app, $module, $key) = $amk;

        $kvs = self::loadMocks($app, $module);

        return isset($kvs[$key]) ? $kvs[$key] : $default;
    }

    /**
     * @param $app
     * @param $module
     * @return mixed
     * @throws \Exception
     */
    private static function loadMocks($app, $module)
    {
        if (isset(self::$data[$app][$module]))
        {
            return self::$data[$app][$module];
        }
        else
        {
            $file = RESOURCE_PATH.'dcc-mocks/'.str_replace('.', '_', $app).'-'.str_replace('.', '_', $module).'.json';
            if (is_file($file))
            {
                $template = json_decode(file_get_contents($file), true);
                if (is_array($template))
                {
                    self::$data[$app][$module] = $template;
                    return $template;
                }
                else
                {
                    throw new \Exception('DCC-mock data invalid ~ '.$file);
                }
            }
            else
            {
                throw new \Exception('DCC-mock data not found ~ '.$file);
            }
        }
    }
}