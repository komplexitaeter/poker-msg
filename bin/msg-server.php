<?php
use Ratchet\WebSocket\WsServer;
use Ratchet\Http\HttpServer;
use Ratchet\Server\IoServer;
use PokerMsg\MsgServer;

require dirname(__DIR__) . '/vendor/autoload.php';

// WebSocket port (nginx forwards /poker-msg to it). Only changed for tests.
$wsPort = (int)(getenv('POKER_MSG_PORT') ?: 8443);

// Ping every client in this interval (seconds); browsers answer automatically.
// Connections that did not answer the previous ping are closed, so dead connections
// (closed tabs, sleeping laptops, connections dropped by proxies) are gone after 1-2 intervals.
$keepAliveInterval = (int)(getenv('POKER_MSG_KEEPALIVE') ?: 30);

// B-81: send the text message "hb" to every client in this interval (seconds, decimals allowed for tests).
// Unlike ping/pong this is visible to the page's JavaScript, so a client can detect a dead connection.
$heartbeatInterval = (float)(getenv('POKER_MSG_HEARTBEAT') ?: 30);

// B-81: local UDP port for notifications from the web server ("pull:<team_id>"), bound to 127.0.0.1 only.
$notifyPort = (int)(getenv('POKER_MSG_NOTIFY_PORT') ?: 8444);

$msgServer = new MsgServer;
$ws = new WsServer($msgServer);
$server = IoServer::factory(new HttpServer($ws), $wsPort, '127.0.0.1');
$ws->enableKeepAlive($server->loop, $keepAliveInterval);

if ($heartbeatInterval > 0) {
    $server->loop->addPeriodicTimer($heartbeatInterval, function () use ($msgServer) {
        $msgServer->heartbeat();
    });
}

// A failing bind (e.g. port in use) is logged; the WebSocket server keeps running without the notify channel.
$udp = @stream_socket_server("udp://127.0.0.1:$notifyPort", $errno, $errstr, STREAM_SERVER_BIND);
if ($udp === false) {
    $msgServer->logMsg("ERROR: notify channel udp://127.0.0.1:$notifyPort not available ($errno: $errstr), continuing without it");
} else {
    stream_set_blocking($udp, false);
    $server->loop->addReadStream($udp, function ($udp) use ($msgServer) {
        // One datagram per call (the loop calls again while more are queued). 256 bytes is plenty for
        // "pull:" + 80 characters; longer datagrams are truncated and then rejected by the length check.
        $data = @stream_socket_recvfrom($udp, 256);
        if (is_string($data) && $data !== '') {
            try {
                $msgServer->onNotify($data);
            } catch (\Throwable $e) {
                $msgServer->logMsg("ERROR: notification failed: {$e->getMessage()}");
            }
        }
    });
    $msgServer->logMsg("Notify channel listening on udp://127.0.0.1:$notifyPort");
}

$msgServer->logMsg("WebSocket server listening on 127.0.0.1:$wsPort (keepalive {$keepAliveInterval}s, heartbeat {$heartbeatInterval}s)");
$server->run();
