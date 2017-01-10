<?php
/**
 * Nsq logger
 * User: moyo
 * Date: 5/18/15
 * Time: 6:27 PM
 */

namespace Kdt\Iron\Queue\Adapter\Nsq;

use Kdt\Iron\Log\Log;
use nsqphp\Logger\LoggerInterface;

class Logger implements LoggerInterface
{
    /**
     * Log error
     *
     * @param string|\Exception $msg
     */
    public function error($msg)
    {
        $this->getLogger()->error($this->msg($msg));
    }

    /**
     * Log warn
     *
     * @param string|\Exception $msg
     */
    public function warn($msg)
    {
        $this->getLogger()->warn($this->msg($msg));
    }

    /**
     * Log info
     *
     * @param string|\Exception $msg
     */
    public function info($msg)
    {
        $this->getLogger()->info($this->msg($msg));
    }

    /**
     * Log debug
     *
     * @param string|\Exception $msg
     */
    public function debug($msg)
    {
        $msgOrigin = $this->msg($msg);
        $msgLength = strlen($msgOrigin);
        if ($msgLength > 128)
        {
            $this->getLogger()->debug(substr($msgOrigin, 0, 128), null, ['origin' => $msgOrigin]);
        }
        else
        {
            $this->getLogger()->debug($msgOrigin);
        }
    }

    /**
     * @return \Kdt\Iron\Log\Track\TrackLogger
     */
    private function getLogger()
    {
        return Log::getInstance('php-framework', 'nsq.client');
    }

    /**
     * Msg convert
     *
     * @param string|\Exception $content
     * @return string
     */
    private function msg($content)
    {
        return $content instanceof \Exception ? $content->getMessage() : (string)$content;
    }
}