<?php
/**
 * Nsq logger
 * User: moyo
 * Date: 5/18/15
 * Time: 6:27 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use nsqphp\Logger\LoggerInterface;

class Logger implements LoggerInterface
{
    private static $type = 'php';
    
    /**
     * Log error
     *
     * @param string|\Exception $msg
     */
    public function error($msg)
    {
        $this->log('error', $msg);
    }

    /**
     * Log warn
     *
     * @param string|\Exception $msg
     */
    public function warn($msg)
    {
        $this->log('warn', $msg);
    }

    /**
     * Log info
     *
     * @param string|\Exception $msg
     */
    public function info($msg)
    {
        $this->log('info', $msg);
    }

    /**
     * Log debug
     *
     * @param string|\Exception $msg
     */
    public function debug($msg)
    {
        $this->log('debug', $msg);
    }

    private function log($level, $msg)
    {
        $s = $msg instanceof \Exception ? $msg->getMessage() : (string)$msg;
        switch ($this->type)
        {
        case 'php':
            error_log(strtoupper($level).' '.$s);
            break;
        case 'stderr':
            fwrite(STDERR, sprintf('[%s] %s: %s%s', date('Y-m-d H:i:s'), strtoupper($level), $s, PHP_EOL));
            break;
        } 
    }

    private function setType($type)
    {
        $this->type = $type;
    }
}
