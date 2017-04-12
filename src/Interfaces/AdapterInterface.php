<?php
/**
 * Adapter interface
 * User: moyo
 * Date: 5/11/15
 * Time: 3:17 PM
 */

namespace Kdt\Iron\Queue\Interfaces;

interface AdapterInterface
{
    /**
     * @param $topic
     * @param $message
     * @return array
     */
    public function push($topic, $message);

    /**
     * @param $topic
     * @param $messages
     * @return array
     */
    public function bulk($topic, array $messages);

    /**
     * @param $topic
     * @param callable $callback
     * @param $options
     */
    public function pop($topic, callable $callback, array $options = []);

    /**
     * @param $messageId
     * @return bool
     */
    public function delete($messageId);

    /**
     * make delay
     * @param $seconds
     */
    public function later($seconds);

    /**
     * make retry
     */
    public function retry();
}