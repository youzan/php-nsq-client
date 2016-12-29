<?php
/**
 * Reader test
 * User: moyo
 * Date: 29/12/2016
 * Time: 5:16 PM
 */

namespace Kdt\Iron\Queue\Tests\nsqphp\Wire;

use nsqphp\Wire\Reader;

class ReaderTest extends \PHPUnit_Framework_TestCase
{
    public function testConstants()
    {
        $ftBroken = -1;
        $ftResponse = 0;
        $ftError = 1;
        $ftMessage = 2;

        $this->assertEquals($ftBroken, Reader::FRAME_TYPE_BROKEN);
        $this->assertEquals($ftResponse, Reader::FRAME_TYPE_RESPONSE);
        $this->assertEquals($ftError, Reader::FRAME_TYPE_ERROR);
        $this->assertEquals($ftMessage, Reader::FRAME_TYPE_MESSAGE);

        $sgHeartbeat = '_heartbeat_';
        $sgOK = 'OK';

        $this->assertEquals($sgHeartbeat, Reader::HEARTBEAT);
        $this->assertEquals($sgOK, Reader::OK);
    }
}