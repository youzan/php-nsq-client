<?php

namespace ZanPHP\Component\ServiceChain;


class ServiceChain
{
    const CFG_KEY = "kdt.X-Service-Chain";
    const ENV_KEY = "KDT_X_SERVICE_CHAIN";
    const HDR_KEY = "X-Service-Chain";
    const CTX_KEY = "service.chain";

    public static function get($parsed = false)
    {
        if (PHP_SAPI === 'cli') {
            $chain = self::fromEnv();
        } else {
            $chain = self::fromHeader();
        }

        if ($parsed) {
            return static::parse($chain);
        } else {
            return $chain;
        }
    }

    private static function fromEnv()
    {
        $chain = getenv(static::ENV_KEY);
        if ($chain !== false) {
            return $chain;
        }

        $chain = get_cfg_var(static::CFG_KEY);
        if ($chain !== false) {
            return $chain;
        }

        return null;
    }

    private static function fromHeader()
    {
        $key = str_replace('-', '_', static::HDR_KEY);
        $key = "HTTP_" . strtoupper($key);
        return isset($_SERVER[$key]) ? $_SERVER[$key] : null;
    }

    private static function parse($raw)
    {
        if (is_string($raw) && preg_match('/^\s*[\[|\{].*[\]|\}\s*$]/', $raw)) {
            $arr = json_decode($raw, true) ?: [];
            if (isset($arr["name"]) && is_string($arr["name"])) {
                return $arr["name"];
            }
        }
        return null;
    }
}

