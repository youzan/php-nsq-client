<?php
/**
 * Event callback test
 * User: moyo
 * Date: 22/12/2016
 * Time: 5:28 PM
 */

namespace Kdt\Iron\Queue\Tests\Foundation\Traits;

use Kdt\Iron\Queue\Exception\ServiceInitializationException;
use Kdt\Iron\Queue\Foundation\Traits\EventCallback;

class EventCallbackTest extends \PHPUnit_Framework_TestCase
{
    use EventCallback;

    public function testTrigger()
    {
        $evName = 'test-event';
        $evName2 = 'test-event-2';

        $executed = false;
        $this->registerEvent($evName, function () use (&$executed) {
            $executed = true;
        });

        $exception = null;
        $this->registerEvent($evName2, function (\Exception $e) use (&$exception) {
            $exception = $e;
        });

        $this->assertEquals(false, $executed);
        $this->triggerEvent($evName);
        $this->assertEquals(true, $executed);

        $this->assertEquals(null, $exception);
        $this->triggerEvent($evName2, new ServiceInitializationException);
        $this->assertInstanceOf(ServiceInitializationException::class, $exception);
    }
}