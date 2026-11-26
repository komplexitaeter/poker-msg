<?php
use Ratchet\WebSocket\WsServer;
use Ratchet\Http\HttpServer;
use Ratchet\Server\IoServer;
use PokerMsg\MsgServer;
use PokerMsg\NotifySocket;
use PokerMsg\WaitServer;

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

// B-151: internal HTTP wait endpoint for MCP wait_for_change (127.0.0.1 only, nginx /_mcp_wait/), 0 = off.
// getenv() instead of "?:" here, because "0" is a valid value.
$waitPort = getenv('POKER_MSG_HTTP_PORT') !== false && getenv('POKER_MSG_HTTP_PORT') !== '' ? (int)getenv('POKER_MSG_HTTP_PORT') : 8445;
// B-151: at most this many concurrent waiters, further requests are answered with r=busy at once.
$maxWait = getenv('POKER_MSG_MCP_MAX_WAIT') !== false && getenv('POKER_MSG_MCP_MAX_WAIT') !== '' ? (int)getenv('POKER_MSG_MCP_MAX_WAIT') : 2000;

$msgServer = new MsgServer;
$ws = new WsServer($msgServer);
$server = IoServer::factory(new HttpServer($ws), $wsPort, '127.0.0.1');
$msgServer->setLoop($server->loop);
$ws->enableKeepAlive($server->loop, $keepAliveInterval);

if ($heartbeatInterval > 0) {
    $server->loop->addPeriodicTimer($heartbeatInterval, function () use ($msgServer) {
        $msgServer->heartbeat();
    });
}

// A failing bind (e.g. port in use) is logged; the WebSocket server keeps running without the notify channel.
$notify = NotifySocket::open($notifyPort);
if ($notify->stream === null) {
    $msgServer->logMsg("ERROR: notify channel udp://127.0.0.1:$notifyPort not available ({$notify->error}), continuing without it");
} else {
    $server->loop->addReadStream($notify->stream, function ($udp) use ($msgServer) {
        // One datagram per call (the loop calls again while more are queued). 256 bytes is plenty for
        // "pull:" + 80 characters; longer datagrams are truncated and then rejected by the length check.
        $data = @stream_socket_recvfrom($udp, 256);
        if (is_string($data) && $data !== '') {
            try {
                $msgServer->onNotify($data);
            } catch (\Throwable $e) {
                $msgServer->logMsg("ERROR: notification failed: " . MsgServer::forLog($e->getMessage(), 300));
            }
        }
    });
    $msgServer->logMsg("Notify channel listening on udp://127.0.0.1:$notifyPort");
    if (!$notify->exclusive) {
        $msgServer->logMsg("WARNING: PHP extension sockets missing, notify port is not bound exclusively (other local processes could bind it too)");
    }
}

if ($waitPort > 0) {
    // A failing bind is logged; WebSocket and UDP keep running (wait_for_change then answers "busy" via nginx).
    try {
        $waitSocket = new React\Socket\Server("127.0.0.1:$waitPort", $server->loop);
        $waitServer = new WaitServer($server->loop, $maxWait, [$msgServer, 'logMsg']);
        new IoServer(new HttpServer($waitServer), $waitSocket, $server->loop);
        $msgServer->setWaitServer($waitServer);
        $server->loop->addPeriodicTimer(WaitServer::LOG_INTERVAL, function () use ($waitServer) {
            $waitServer->logStats();
        });
        $msgServer->logMsg("MCP wait endpoint listening on http://127.0.0.1:$waitPort (max $maxWait waiters)");
    } catch (\Throwable $e) {
        $msgServer->logMsg("ERROR: MCP wait endpoint 127.0.0.1:$waitPort not available (" . MsgServer::forLog($e->getMessage(), 300) . "), continuing without it");
    }
} else {
    $msgServer->logMsg("MCP wait endpoint disabled (POKER_MSG_HTTP_PORT=0)");
}

$msgServer->logMsg("WebSocket server listening on 127.0.0.1:$wsPort (keepalive {$keepAliveInterval}s, heartbeat {$heartbeatInterval}s)");
$server->run();
