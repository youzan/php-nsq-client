<?php

namespace nsqphp\Lookup;

use nsqphp\Exception\LookupException;

interface LookupInterface
{
    /**
     * Lookup hosts for a given topic
     * 
     * @param string $topic
     *
     * @param string $sp value range is [sub|pub]
     * 
     * @throws LookupException If we cannot talk to / get back invalid response
     *      from nsqlookupd
     * 
     * @return array Should return array [] = host:port
     */
    public function lookupHosts($topic = null, $sp = null);
}