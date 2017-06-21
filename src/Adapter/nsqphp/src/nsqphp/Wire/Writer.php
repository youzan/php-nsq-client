<?php

namespace nsqphp\Wire;

class Writer
{
    /**
     * "Magic" identifier - for version we support
     */
    const MAGIC_V2 = "  V2";
    
    /**
     * Magic hello
     * 
     * @return string
     */
    public function magic()
    {
        return self::MAGIC_V2;
    }

    /**
     * Identify self
     *
     * @param $client_id
     * @param $hostname
     * @param $user_agent
     * @param $msg_timeout
     * @return string
     */
    public function identify($params)
    {
        $cmd = $this->command('IDENTIFY');
        extract($params);
        $data = ['client_id' => (string)$client_id, 'hostname' => $hostname, 'user_agent' => $user_agent];
        $msg_timeout = intval($msg_timeout);
        if ($msg_timeout > 0)
        {
            $data['msg_timeout'] = $msg_timeout; 
        }
        if (!empty($desired_tag))
        {
            $data['desired_tag'] = strval($desired_tag);
            //$data['tag_filter'] = strval($desired_tag);
        }
        $json = json_encode($data);
        $size = pack('N', strlen($json));
        return $cmd . $size . $json;
    }
    
    /**
     * Subscribe [SUB]
     * 
     * @param string $topic
     * @param string $channel
     * @param string $partitionID
     * @param bool   $ordered
     *
     * @return string
     */
    public function subscribe($topic, $channel, $partitionID = null, $ordered = false)
    {
        if (is_numeric($partitionID))
        {
            return $this->command($ordered ? 'SUB_ORDERED' : 'SUB', $topic, $channel, $partitionID);
        }
        else
        {
            return $this->command('SUB', $topic, $channel);
        }
    }
    
    /**
     * Publish [PUB]
     * 
     * @param string $topic
     * @param Message $message
     * @param integer $partitionID
     * @param string $traceID
     * 
     * @return string
     */
    public function publish($topic, $messageBag, $partitionID = null, $traceID = null)
    {
        // the fast pack way, but may be unsafe
        // $cmd = $this->command('PUB', $topic);
        // $size = pack('N', strlen($message));
        // return $cmd . $size . $message;
        
        // the safe way, but is time cost
        $payload = $messageBag->getPayload();
        $api = $traceID ? 'PUB_TRACE' : 'PUB';
        $cmdArgs = [$api, $topic];
        if (is_numeric($partitionID))
        {
            array_push($cmdArgs, $partitionID);
        }
        $tag = $messageBag->getTag();
        if (!empty($tag))
        {
            array_push($cmdArgs, $tag);
        }
        $cmd = call_user_func_array([$this, 'command'], $cmdArgs);
        $data = ($traceID ? $this->packLong($traceID) : '') . $this->packString($payload);
        $size = pack('N', strlen($data));

        return $cmd . $size . $data;
    }

    /**
     * Publish [PUB] via HTTP
     * @param string $topic
     * @param Message $message
     * @param integer $partitionID
     * @param string $traceID
     * @return array
     */
    public function publishForHttp($topic, $message, $partitionID = null, $traceID = null)
    {
        $api = $traceID ? ('/pubtrace?trace_id='.$traceID.'&') : '/pub?';

        $api .= 'topic='.rawurlencode($topic);
        $tag = $message->getTag();
        if (!empty($tag)) {
            $api .= 'tag='.rawurlencode($tag);
        }
        is_numeric($partitionID) && $api .= '&partition='.$partitionID;
        $payload = $messageBag->getPayload();
        $data = $this->packString($payload);

        return [
            $api,
            $data
        ];
    }

    /**
     * MultiPublish [MPUB]
     * @param $topic
     * @param $messages
     *
     * @return string
     */
    public function multiPublish($topic, $messages)
    {
        $cmd = $this->command('MPUB', $topic);

        $msgNum = pack('N', count($messages));

        $buffer = '';

        foreach ($messages as $message)
        {
            $data = $this->packString($message);
            $size = pack('N', strlen($data));
            $buffer .= $size.$data;
        }

        $bodySize = pack('N', strlen($msgNum.$buffer));

        return $cmd . $bodySize . $msgNum . $buffer;
    }

    /**
     * MultiPublish [MPUB] via HTTP
     * @param $topic
     * @param $messages
     * @return array
     */
    public function multiPublishForHttp($topic, $messages)
    {
        $api = '/mpub?topic='.$topic.'&binary=true';

        $msgNum = pack('N', count($messages));

        $buffer = '';

        foreach ($messages as $message)
        {
            $data = $this->packString($message);
            $size = pack('N', strlen($data));
            $buffer .= $size.$data;
        }

        return [
            $api,
            $msgNum . $buffer
        ];
    }

    /**
     * Ready [RDY]
     * 
     * @param integer $count
     * 
     * @return string
     */
    public function ready($count)
    {
        return $this->command('RDY', $count);
    }
    
    /**
     * Finish [FIN]
     * 
     * @param string $id
     * 
     * @return string
     */
    public function finish($id)
    {
        // special code because $id maybe is binary
        return 'FIN '.$id."\n";
    }

    /**
     * Requeue [REQ]
     *
     * @param string $id
     * @param integer $timeMs
     * 
     * @return string
     */
    public function requeue($id, $timeMs)
    {
        // special code because $id maybe is binary
        return 'REQ '.$id.' '.$timeMs."\n";
    }
    
    /**
     * No-op [NOP]
     *
     * @return string
     */
    public function nop()
    {
        return $this->command('NOP');
    }
    
    /**
     * Cleanly close [CLS]
     *
     * @return string
     */
    public function close()
    {
        return $this->command('CLS');
    }
        
    /**
     * Command
     * 
     * @return string
     */
    private function command()
    {
        $args = func_get_args();
        $cmd = array_shift($args);
        return sprintf("%s %s%s", $cmd, implode(' ', $args), "\n");
    }
    
    /**
     * Pack string -> binary
     *
     * @param string $str
     * 
     * @return string Binary packed
     */
    private function packString($str)
    {        
        $outStr = "";
        $len = strlen($str);
        for ($i = 0; $i < $len; $i++) {
            $outStr .= pack("c", ord(substr($str, $i, 1))); 
        } 
        return $outStr; 
    }

    /**
     * @param $value
     *
     * @return string
     */
    private function packLong($value)
    {
        // If we are on a 32bit architecture we have to explicitly deal with
        // 64-bit twos-complement arithmetic since PHP wants to treat all ints
        // as signed and any int over 2^31 - 1 as a float
        if (PHP_INT_SIZE == 4) {
            $neg = $value < 0;

            if ($neg) {
                $value *= -1;
            }

            $hi = (int) ($value / 4294967296);
            $lo = (int) $value;

            if ($neg) {
                $hi = ~$hi;
                $lo = ~$lo;
                if (($lo & (int) 0xffffffff) == (int) 0xffffffff) {
                    $lo = 0;
                    $hi++;
                } else {
                    $lo++;
                }
            }
            $data = pack('N2', $hi, $lo);

        } else {
            $hi = $value >> 32;
            $lo = $value & 0xFFFFFFFF;
            $data = pack('N2', $hi, $lo);
        }

        return $data;
    }
}
