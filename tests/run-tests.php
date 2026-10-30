<?php
/*
 * Integration tests for poker-msg (B-81): starts bin/msg-server.php on test ports,
 * connects minimal WebSocket clients and checks pull distribution, UDP notifications,
 * the "hb" heartbeat and that a failing UDP bind does not stop the server.
 *
 * Usage: php tests/run-tests.php   (needs vendor/, ports 18443/18444 and 18445/18446 free)
 * Exit code 0 = all passed.
 */

const WS_PORT = 18443;
const NOTIFY_PORT = 18444;

require dirname(__DIR__) . '/vendor/autoload.php';

use PokerMsg\MsgServer;

$failures = 0;
function check(bool $ok, string $name): void {
    global $failures;
    echo ($ok ? "  ok   " : "  FAIL ") . $name . "\n";
    if (!$ok) $failures++;
}

/* ---------- minimal WebSocket client (text frames, answers pings) ---------- */

final class WsClient {
    /** @var resource */
    private $sock;
    private string $buf = '';

    public function __construct(int $port) {
        $sock = stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 2);
        if ($sock === false) throw new RuntimeException("connect failed: $errstr");
        $this->sock = $sock;
        $key = base64_encode(random_bytes(16));
        fwrite($sock, "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Key: $key\r\nSec-WebSocket-Version: 13\r\n\r\n");
        $resp = '';
        $deadline = microtime(true) + 2;
        while (!str_contains($resp, "\r\n\r\n") && microtime(true) < $deadline) {
            $chunk = fread($sock, 1);
            if ($chunk === '' || $chunk === false) { usleep(1000); continue; }
            $resp .= $chunk;
        }
        if (!str_contains($resp, ' 101 ')) throw new RuntimeException("handshake failed: $resp");
        stream_set_blocking($sock, false);
    }

    public function send(string $text): void {
        $mask = random_bytes(4);
        $len = strlen($text);
        $head = chr(0x81) . ($len < 126 ? chr(0x80 | $len) : chr(0x80 | 126) . pack('n', $len));
        $payload = '';
        for ($i = 0; $i < $len; $i++) $payload .= $text[$i] ^ $mask[$i % 4];
        fwrite($this->sock, $head . $mask . $payload);
    }

    private function sendControl(int $opcode, string $payload): void {
        $mask = random_bytes(4);
        $masked = '';
        for ($i = 0; $i < strlen($payload); $i++) $masked .= $payload[$i] ^ $mask[$i % 4];
        fwrite($this->sock, chr(0x80 | $opcode) . chr(0x80 | strlen($payload)) . $mask . $masked);
    }

    /** Collects text messages received within $seconds. */
    public function receive(float $seconds): array {
        $msgs = [];
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $r = [$this->sock]; $w = null; $e = null;
            $left = max(0, $deadline - microtime(true));
            if (@stream_select($r, $w, $e, (int)$left, (int)(($left - (int)$left) * 1e6)) > 0) {
                $chunk = fread($this->sock, 8192);
                if ($chunk === '' && feof($this->sock)) break;
                $this->buf .= (string)$chunk;
            }
            while (($frame = $this->nextFrame()) !== null) {
                [$opcode, $payload] = $frame;
                if ($opcode === 0x1) $msgs[] = $payload;
                if ($opcode === 0x9) $this->sendControl(0xA, $payload); // ping -> pong
            }
        }
        return $msgs;
    }

    private function nextFrame(): ?array {
        if (strlen($this->buf) < 2) return null;
        $opcode = ord($this->buf[0]) & 0x0F;
        $len = ord($this->buf[1]) & 0x7F;
        $off = 2;
        if ($len === 126) { if (strlen($this->buf) < 4) return null; $len = unpack('n', substr($this->buf, 2, 2))[1]; $off = 4; }
        if ($len === 127) { if (strlen($this->buf) < 10) return null; $len = unpack('J', substr($this->buf, 2, 8))[1]; $off = 10; }
        if (strlen($this->buf) < $off + $len) return null;
        $payload = substr($this->buf, $off, $len);
        $this->buf = substr($this->buf, $off + $len);
        return [$opcode, $payload];
    }

    public function close(): void { fclose($this->sock); }
}

/* ---------- helpers ---------- */

/** @return array{0: resource, 1: string} process and log file */
function startServer(int $wsPort, int $notifyPort, string $heartbeat): array {
    $log = tempnam(sys_get_temp_dir(), 'poker-msg-test');
    $env = array_merge(getenv(), [
        'POKER_MSG_PORT' => (string)$wsPort,
        'POKER_MSG_NOTIFY_PORT' => (string)$notifyPort,
        'POKER_MSG_HEARTBEAT' => $heartbeat,
        'POKER_MSG_KEEPALIVE' => '60',
    ]);
    $cmd = [PHP_BINARY, dirname(__DIR__) . '/bin/msg-server.php'];
    $proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['file', $log, 'w'], 2 => ['file', $log, 'a']], $pipes, null, $env);
    if (!is_resource($proc)) throw new RuntimeException('could not start server');
    $deadline = microtime(true) + 5;
    while (microtime(true) < $deadline) {
        $s = @stream_socket_client("tcp://127.0.0.1:$wsPort", $errno, $errstr, 0.2);
        if ($s !== false) { fclose($s); return [$proc, $log]; }
        usleep(100000);
    }
    throw new RuntimeException("server did not start:\n" . file_get_contents($log));
}

function stopServer($proc): void {
    proc_terminate($proc);
    proc_close($proc);
}

function udp(int $port, string $msg): void {
    $s = stream_socket_client("udp://127.0.0.1:$port", $errno, $errstr);
    fwrite($s, $msg);
    fclose($s);
}

function subscribed(int $port, string $team): WsClient {
    $c = new WsClient($port);
    $c->send("subscribe: $team");
    return $c;
}

/* ---------- unit: message parsing ---------- */

echo "parseNotify\n";
check(MsgServer::parseNotify('pull:Team-1_ab') === 'Team-1_ab', 'valid message');
check(MsgServer::parseNotify("pull:abc\n") === 'abc', 'trailing newline is trimmed');
check(MsgServer::parseNotify('pull:') === null, 'empty team id is ignored');
check(MsgServer::parseNotify('push:abc') === null, 'other prefix is ignored');
check(MsgServer::parseNotify('pull:' . str_repeat('a', 80)) === str_repeat('a', 80), '80 characters accepted');
check(MsgServer::parseNotify('pull:' . str_repeat('a', 81)) === null, '81 characters rejected');
check(MsgServer::parseNotify("pull:a b") === null, 'whitespace rejected');
check(MsgServer::parseNotify("pull:a\x00b") === null, 'control characters rejected');
check(MsgServer::parseNotify("pull:t\xC3\xA4m") === null, 'non-ASCII rejected');

/* ---------- integration: running server ---------- */

echo "server (ws " . WS_PORT . ", udp " . NOTIFY_PORT . ", heartbeat 1s)\n";
[$proc, $log] = startServer(WS_PORT, NOTIFY_PORT, '1');
try {
    $a1 = subscribed(WS_PORT, 'teamA');
    $a2 = subscribed(WS_PORT, 'teamA');
    $b1 = subscribed(WS_PORT, 'teamB');
    usleep(200000);
    // drain heartbeats that may already have arrived
    foreach ([$a1, $a2, $b1] as $c) $c->receive(0.05);

    udp(NOTIFY_PORT, 'pull:teamA');
    $ra1 = $a1->receive(0.5); $ra2 = $a2->receive(0.1); $rb1 = $b1->receive(0.1);
    check(in_array('pull', $ra1, true) && in_array('pull', $ra2, true), 'UDP pull:teamA reaches both teamA clients');
    check(!in_array('pull', $rb1, true), 'teamB client gets no pull');

    $a1->send('push');
    $ra1 = $a1->receive(0.5); $ra2 = $a2->receive(0.1); $rb1 = $b1->receive(0.1);
    check(in_array('pull', $ra1, true) && in_array('pull', $ra2, true), 'browser push still reaches the team (incl. sender)');
    check(!in_array('pull', $rb1, true), 'browser push does not reach other teams');

    udp(NOTIFY_PORT, 'garbage');
    udp(NOTIFY_PORT, 'pull:' . str_repeat('x', 300));
    udp(NOTIFY_PORT, "pull:\x00\xff");
    udp(NOTIFY_PORT, 'pull:teamB');
    $rb1 = $b1->receive(0.5);
    check(in_array('pull', $rb1, true), 'server still works after invalid datagrams');

    $hb = $b1->receive(2.3);
    $hbCount = count(array_filter($hb, fn($m) => $m === 'hb'));
    check($hbCount >= 2, "heartbeat 'hb' arrives every interval (got $hbCount in 2.3s)");
    check(!in_array('pull', $hb, true), "no pull without a change");

    $unsub = new WsClient(WS_PORT);
    check(in_array('hb', $unsub->receive(1.3), true), "unsubscribed connection gets 'hb' too");
    $unsub->close();

    foreach ([$a1, $a2, $b1] as $c) $c->close();
    usleep(200000);
    $logText = file_get_contents($log);
    check(!preg_match('/\bhb\b/', $logText), 'heartbeats are not logged');
    check(str_contains($logText, "Notify channel listening on udp://127.0.0.1:" . NOTIFY_PORT), 'notify channel logged at start');
    check(str_contains($logText, 'Ignored invalid notification'), 'invalid datagram logged');
} finally {
    stopServer($proc);
}

echo "bind error (udp port already in use)\n";
// Block the port without SO_REUSEADDR (stream_socket_server sets it, so a second bind would succeed).
if (!function_exists('socket_create')) {
    echo "  skip (sockets extension missing)\n";
    echo $failures === 0 ? "ALL PASSED\n" : "$failures FAILED\n";
    exit($failures === 0 ? 0 : 1);
}
$blocker = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
socket_bind($blocker, '127.0.0.1', 18446);
[$proc, $log] = startServer(18445, 18446, '0');
try {
    $c1 = subscribed(18445, 'teamC');
    $c2 = subscribed(18445, 'teamC');
    usleep(200000);
    $c1->send('push');
    check(in_array('pull', $c2->receive(0.5), true), 'WebSocket server keeps working without notify channel');
    check(str_contains(file_get_contents($log), 'ERROR: notify channel udp://127.0.0.1:18446 not available'), 'bind error is logged');
    $c1->close(); $c2->close();
} finally {
    stopServer($proc);
    socket_close($blocker);
}

echo $failures === 0 ? "ALL PASSED\n" : "$failures FAILED\n";
exit($failures === 0 ? 0 : 1);
