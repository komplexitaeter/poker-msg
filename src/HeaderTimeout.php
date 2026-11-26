<?php
namespace PokerMsg;

use Exception;
use Ratchet\ConnectionInterface;
use Ratchet\MessageComponentInterface;
use React\EventLoop\LoopInterface;

/**
 * B-151: closes connections of the wait endpoint that have not sent complete HTTP headers within $seconds
 * (nginx sends them at once). Wraps Ratchet's HttpServer, which sets $conn->httpHeadersReceived.
 */
final class HeaderTimeout implements MessageComponentInterface {
    /** @var array<int, \React\EventLoop\TimerInterface> resource id => timer */
    private array $timers = [];

    public function __construct(
        private MessageComponentInterface $app,
        private LoopInterface $loop,
        private float $seconds = 5.0,
    ) {}

    public function onOpen(ConnectionInterface $conn) {
        $id = $conn->resourceId;
        $this->timers[$id] = $this->loop->addTimer($this->seconds, function () use ($conn, $id) {
            unset($this->timers[$id]);
            if (($conn->httpHeadersReceived ?? false) !== true) {
                $conn->close();
            }
        });
        $this->app->onOpen($conn);
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        $this->app->onMessage($from, $msg);
        if (($from->httpHeadersReceived ?? false) === true) $this->cancel($from);
    }

    public function onClose(ConnectionInterface $conn) {
        $this->cancel($conn);
        $this->app->onClose($conn);
    }

    public function onError(ConnectionInterface $conn, Exception $e) {
        $this->cancel($conn);
        $this->app->onError($conn, $e);
    }

    private function cancel(ConnectionInterface $conn): void {
        $id = $conn->resourceId;
        if (isset($this->timers[$id])) {
            $this->loop->cancelTimer($this->timers[$id]);
            unset($this->timers[$id]);
        }
    }
}
