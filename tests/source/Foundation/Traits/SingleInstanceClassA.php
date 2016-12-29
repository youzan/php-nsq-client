<?php
/**
 * Test class
 * User: moyo
 * Date: 22/12/2016
 * Time: 5:40 PM
 */

namespace Kdt\Iron\Queue\Tests\Foundation\Traits;

use Kdt\Iron\Queue\Foundation\Traits\SingleInstance;

class SingleInstanceClassA
{
    use SingleInstance;

    /**
     * @var int
     */
    public $idx = 0;

    /**
     * SingleInstanceClassA constructor.
     */
    public function __construct()
    {
        $this->idx = uniqid();
    }
}