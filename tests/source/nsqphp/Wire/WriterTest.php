<?php
/**
 * Writer test
 * User: moyo
 * Date: 29/12/2016
 * Time: 5:17 PM
 */

namespace Kdt\Iron\Queue\Tests\nsqphp\Wire;

use nsqphp\Wire\Writer;

class WriterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Writer
     */
    private $writerIns = null;

    public function testMagic()
    {
        $magic = "  V2";

        $this->assertEquals($magic, $this->w()->magic());
    }

    /**
     * @return Writer
     */
    private function w()
    {
        if (is_null($this->writerIns))
        {
            $this->writerIns = new Writer;
        }
        return $this->writerIns;
    }
}