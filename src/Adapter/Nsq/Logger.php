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
     * @var string
     */
    private $runEnv = 'online';

    /**
     * Logger constructor.
     */
    public function __construct()
    {
        $this->runEnv = Config::getInstance()->getGlobalSetting('run_mode');
    }

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
        if ($this->runEnv == 'test')
        {
            $msgOrigin = $this->msg($msg);
            $msgShort = substr($msgOrigin, 0, 128);
            $this->getLogger()->debug($msgShort, null, $msgOrigin);
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