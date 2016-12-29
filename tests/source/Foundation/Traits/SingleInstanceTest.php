<?php
/**
 * Single instance test
 * User: moyo
 * Date: 22/12/2016
 * Time: 5:38 PM
 */

namespace Kdt\Iron\Queue\Tests\Foundation\Traits;

class SingleInstanceTest extends \PHPUnit_Framework_TestCase
{
    public function testInstance()
    {
        $idx1 = SingleInstanceClassA::getInstance()->idx;
        $idx2 = SingleInstanceClassA::getInstance()->idx;
        $idx3 = SingleInstanceClassA::newInstance()->idx;

        $this->assertEquals($idx1, $idx2);
        $this->assertNotEquals($idx2, $idx3);

        $classA = SingleInstanceClassA::getInstance();
        $classB = SingleInstanceClassB::getInstance();

        $this->assertNotEquals($classA, $classB);
    }
}