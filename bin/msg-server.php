<?php
use Ratchet\WebSocket\WsServer;
use Ratchet\Http\HttpServer;
use Ratchet\Server\IoServer;
use PokerMsg\MsgServer;

require dirname(__DIR__) . '/vendor/autoload.php';

// Ping every client in this interval (seconds); browsers answer automatically.
// Connections that did not answer the previous ping are closed, so dead connections
// (closed tabs, sleeping laptops, connections dropped by proxies) are gone after 1-2 intervals.
$keepAliveInterval = (int)(getenv('POKER_MSG_KEEPALIVE') ?: 30);

$ws = new WsServer(new MsgServer);
$server = IoServer::factory(new HttpServer($ws), 8443, '127.0.0.1');
$ws->enableKeepAlive($server->loop, $keepAliveInterval);
$server->run();
