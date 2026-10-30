<?php
namespace PokerMsg;

/**
 * B-81: opens the local UDP notification socket on 127.0.0.1.
 *
 * With ext-sockets the port is bound exclusively (without SO_REUSEADDR), so no other local process can bind
 * the same port and receive the notifications. PHP's stream_socket_server() always sets SO_REUSEADDR, which on
 * Linux lets a second socket with the same option share the port; it is only used as fallback.
 */
final class NotifySocket {
    /** @var resource|null non-blocking stream for the event loop */
    public $stream = null;
    /** @var \Socket|null kept to hold the exported socket open */
    public $socket = null;
    public bool $exclusive = false;
    public string $error = '';

    public static function open(int $port): self {
        $result = new self();
        if (function_exists('socket_create')) {
            $socket = @socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
            if ($socket === false) {
                $result->error = socket_strerror(socket_last_error());
                return $result;
            }
            if (!@socket_bind($socket, '127.0.0.1', $port)) {
                $result->error = socket_strerror(socket_last_error($socket));
                socket_close($socket);
                return $result;
            }
            $stream = socket_export_stream($socket);
            if ($stream === false) {
                $result->error = 'socket_export_stream failed';
                socket_close($socket);
                return $result;
            }
            stream_set_blocking($stream, false);
            $result->socket = $socket;
            $result->stream = $stream;
            $result->exclusive = true;
            return $result;
        }

        $stream = @stream_socket_server("udp://127.0.0.1:$port", $errno, $errstr, STREAM_SERVER_BIND);
        if ($stream === false) {
            $result->error = "$errno: $errstr";
            return $result;
        }
        stream_set_blocking($stream, false);
        $result->stream = $stream;
        return $result;
    }
}
