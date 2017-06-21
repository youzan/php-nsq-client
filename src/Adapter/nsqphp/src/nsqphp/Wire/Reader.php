<?php

namespace nsqphp\Wire;

use nsqphp\Connection\ConnectionInterface;
use nsqphp\Exception\ProtocolException;
use nsqphp\Exception\SocketException;
use nsqphp\Exception\ReadException;
use nsqphp\Exception\ErrorFrameException;
use nsqphp\Exception\ResponseFrameException;
use nsqphp\Exception\UnknownFrameException;

class Reader
{
    /**
     * Frame types
     */
    const FRAME_TYPE_BROKEN = -1;
    const FRAME_TYPE_RESPONSE = 0;
    const FRAME_TYPE_ERROR = 1;
    const FRAME_TYPE_MESSAGE = 2;
    
    /**
     * Heartbeat response content
     */
    const HEARTBEAT = '_heartbeat_';
    
    /**
     * OK response content
     */
    const OK = 'OK';

    /**
     * Read frame
     *
     * @param ConnectionInterface $connection
     * @throws ReadException If we have a problem reading the core frame header
     *      (data size + frame type)
     * @throws ReadException If we have a problem reading the frame data
     * 
     * @return array With keys: type, data
     */
    public function readFrame(ConnectionInterface $connection)
    {
        $size = $frameType = NULL;
        try {
            $size = $this->readInt($connection);
            $frameType = $this->readInt($connection);
        } catch (SocketException $e) {
            throw new ReadException("Error reading message frame [$size, $frameType] (" . $e->getMessage() . ")", NULL, $e);
        }

        $frame = array(
            'type'  => $frameType,
            'size'  => $size
            );
        
        try {
            switch ($frameType) {
                case self::FRAME_TYPE_RESPONSE:
                    $rpsLEN = $size - 4;

                    $data = null;

                    $hbTXT = self::HEARTBEAT;
                    $hbLEN = strlen(self::HEARTBEAT);

                    if ($rpsLEN == $hbLEN)
                    {
                        // maybe packet is heartbeat
                        $rpsTXT = $this->readString($connection, $hbLEN);

                        if ($rpsTXT == $hbTXT)
                        {
                            $data = $rpsTXT;
                        }
                        else
                        {
                            throw new ProtocolException('Unexpected data received');
                        }
                    }
                    else
                    {
                        // mostly packet is OK
                        $okTXT = self::OK;
                        $okLEN = strlen(self::OK);

                        $rpsTXT = $this->readString($connection, $okLEN);
                        if ($rpsTXT == $okTXT && $rpsLEN == $okLEN)
                        {
                            // response is expect .. "OK"
                            $data = $rpsTXT;
                        }
                        else
                        {
                            if ($rpsLEN > $okLEN)
                            {
                                if ($connection->isYzCluster())
                                {
                                    $data = $rpsTXT;

                                    // currently unused

                                    $_internalID = $this->readBinary($connection, 8);
                                    $_traceID = $this->readBinary($connection, 8);
                                    $_diskQueueOffset = $this->readBinary($connection, 8);
                                    $_diskQueueSize = $this->readBinary($connection, 4);
                                }
                                else
                                {
                                    $data = $rpsTXT . $this->readString($connection, $rpsLEN - $okLEN);
                                }
                            }
                            else
                            {
                                // response is 2 characters
                                $data = $rpsTXT;
                            }
                        }
                    }
                    $frame['response'] = $data;
                    break;
                case self::FRAME_TYPE_ERROR:
                    $frame['error'] = $this->readString($connection, $size-4);
                    break;
                case self::FRAME_TYPE_MESSAGE:
                    $frame['ts'] = $this->readLong($connection);
                    $frame['attempts'] = $this->readShort($connection);
                    if ($connection->isYzCluster()) {
                        $id = $this->readBinary($connection, 16);
                        $frame['dsp_id'] = bin2hex($id);
                        $frame['raw_id'] = $id;
                        $frame['trace_id'] = $this->unpackLong(substr($id, 8, 4), substr($id, 12, 4));
                    } else {
                        $id = $this->readString($connection, 16);
                        $frame['dsp_id'] = $id;
                        $frame['raw_id'] = $id;
                        $frame['trace_id'] = 0;
                    }
                    // 30 = frame type [4] + timestamp [8] + attempts [2] + message id [16]
                    $dataOffset = 30;
                    if ($connection->getHasExtendData()) 
                    {
                        $extVer = ord($this->readBinary($connection, 1));
                        switch ($extVer)
                        {
                        case 2:
                            $extSize = $this->readShort($connection);
                            $dataOffset+= 3 + $extSize;
                            $frame['tag'] = $this->readString($connection, $extSize);
                            break;
                        default:
                            throw new ProtocolException('Unsupported ext version: '.$extVer); 
                        }
                    }
                    // payload offset for ext-info in ordered subscribe
                    $payloadOffset = 0;
                    if ($connection->isOrderedSub())
                    {
                        $payloadOffset = 12;

                        // currently unused

                        $_diskQueueOffset = $this->readBinary($connection, 8);
                        $_diskQueueSize = $this->readBinary($connection, 4);
                    }

                    $frame['payload'] = $this->readString($connection, $size - $dataOffset - $payloadOffset);
                    break;
                default:
                    throw new UnknownFrameException($this->readString($connection, $size-4));
                    break;
            }
            // check frame data
            foreach ($frame as $k => $val)
            {
                if (is_null($val))
                {
                    $frame['type'] = self::FRAME_TYPE_BROKEN;
                    $frame['error'] = 'broken frame (maybe network error)';
                    break;
                }
            }
        } catch (SocketException $e) {
            throw new ReadException("Error reading frame details [$size, $frameType]", NULL, $e);
        }

        return $frame;
    }
    
    /**
     * Test if frame is a response frame (optionally with content $response)
     *
     * @param array $frame
     * @param string|NULL $response If provided we'll check for this specific
     *      response
     * 
     * @return boolean
     */
    public function frameIsResponse(array $frame, $response = NULL)
    {
        return isset($frame['type'], $frame['response'])
                && $frame['type'] === self::FRAME_TYPE_RESPONSE
                && ($response === NULL || $frame['response'] === $response);
    }

    /**
     * Test if frame is a message frame
     *
     * @param array $frame
     * 
     * @return boolean
     */
    public function frameIsMessage(array $frame)
    {
        return isset($frame['type'], $frame['payload'])
                && $frame['type'] === self::FRAME_TYPE_MESSAGE;
    }
    
    /**
     * Test if frame is heartbeat
     * 
     * @param array $frame
     * 
     * @return boolean
     */
    public function frameIsHeartbeat(array $frame)
    {
        return $this->frameIsResponse($frame, self::HEARTBEAT);
    }

    /**
     * Test if frame is OK
     * 
     * @param array $frame
     * 
     * @return boolean
     */
    public function frameIsOk(array $frame)
    {
        return $this->frameIsResponse($frame, self::OK);
    }

    /**
     * @param array $frame
     * @return bool
     */
    public function frameIsError(array $frame)
    {
        return isset($frame['type'])
                && $frame['type'] === self::FRAME_TYPE_ERROR
                && isset($frame['error']);
    }

    /**
     * Test if frame is Broken
     * @param array $frame
     * @return bool
     */
    public function frameIsBroken(array $frame)
    {
        return isset($frame['type'])
                && $frame['type'] === self::FRAME_TYPE_BROKEN;
    }
    
    /**
     * Read and unpack short integer (2 bytes) from connection
     *
     * @param ConnectionInterface $connection
     * 
     * @return integer
     */
    private function readShort(ConnectionInterface $connection)
    {
        $unpack = @unpack('n', $connection->read(2));
        if (is_array($unpack))
        {
            list(,$res) = $unpack;
            return $res;
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Read and unpack integer (4 bytes) from connection
     *
     * @param ConnectionInterface $connection
     * 
     * @return integer
     */
    private function readInt(ConnectionInterface $connection)
    {
        $unpack = @unpack('N', $connection->read(4));
        if (is_array($unpack))
        {
            list(,$res) = $unpack;
            if ((PHP_INT_SIZE !== 4)) {
                $res = sprintf("%u", $res);
            }
            return (int)$res;
        }
        else
        {
            return null;
        }
    }

    /**
     * Read and unpack long (8 bytes) from connection
     *
     * @param ConnectionInterface $connection
     * 
     * @return string We return as string so it works on 32 bit arch
     */
    private function readLong(ConnectionInterface $connection)
    {
        return $this->unpackLong($connection->read(4), $connection->read(4));
    }

    /**
     * @param $h
     * @param $l
     *
     * @return null|string
     */
    private function unpackLong($h, $l)
    {
        $hi = @unpack('N', $h);
        $lo = @unpack('N', $l);

        if (is_array($hi) && is_array($lo))
        {
            // workaround signed/unsigned braindamage in php
            $hi = sprintf("%u", $hi[1]);
            $lo = sprintf("%u", $lo[1]);

            return bcadd(bcmul($hi, "4294967296" ), $lo);
        }
        else
        {
            return null;
        }
    }

    /**
     * Read binary without any parse
     *
     * @param ConnectionInterface $connection
     * @param $size
     * @return string
     */
    private function readBinary(ConnectionInterface $connection, $size)
    {
        return $connection->read($size);
    }

    /**
     * Read and unpack string; reading $size bytes
     *
     * @param ConnectionInterface $connection
     * @param integer $size
     * 
     * @return string 
     */
    private function readString(ConnectionInterface $connection, $size)
    {
        $temp = @unpack("c{$size}chars", $connection->read($size));
        if (is_array($temp))
        {
            $out = "";
            foreach($temp as $v) {
                if ($v > 0) {
                    $out .= chr($v);
                }
            }
            return $out;
        }
        else
        {
            return null;
        }
    }
}
