<?php
/*
 * Integration tests for poker-msg (B-81): starts bin/msg-server.php on test ports,
 * connects minimal WebSocket clients and checks pull distribution, UDP notifications,
 * reactions (B-110), the "hb" heartbeat, the MCP wait and react endpoints (B-151) and that a failing UDP or HTTP bind
 * does not stop the server.
 *
 * Usage: php tests/run-tests.php   (needs vendor/, ports 18443-18448 free)
 * Exit code 0 = all passed.
 */

const WS_PORT = 18443;
const NOTIFY_PORT = 18444;
const HTTP_PORT = 18447;

require dirname(__DIR__) . '/vendor/autoload.php';

use PokerMsg\MsgServer;
use PokerMsg\WaitServer;

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
function startServer(int $wsPort, int $notifyPort, string $heartbeat, int $httpPort = 0, int $maxWait = 2000): array {
    $log = tempnam(sys_get_temp_dir(), 'poker-msg-test');
    $env = array_merge(getenv(), [
        'POKER_MSG_PORT' => (string)$wsPort,
        'POKER_MSG_NOTIFY_PORT' => (string)$notifyPort,
        'POKER_MSG_HEARTBEAT' => $heartbeat,
        'POKER_MSG_KEEPALIVE' => '60',
        'POKER_MSG_HTTP_PORT' => (string)$httpPort,
        'POKER_MSG_MCP_MAX_WAIT' => (string)$maxWait,
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

/** B-151: opens a request to the wait endpoint like nginx does (GET, no body); returns the socket. */
function waitOpen(int $port, string $path, string $method = 'GET') {
    $s = stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 2);
    if ($s === false) throw new RuntimeException("connect failed: $errstr");
    fwrite($s, "$method $path HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n");
    stream_set_blocking($s, false);
    return $s;
}

/** Reads until the server closes the connection or $seconds pass; returns the response or null if none. */
function waitRead($s, float $seconds): ?string {
    $buf = '';
    $deadline = microtime(true) + $seconds;
    while (microtime(true) < $deadline) {
        $r = [$s]; $w = null; $e = null;
        $left = max(0, $deadline - microtime(true));
        if (@stream_select($r, $w, $e, (int)$left, (int)(($left - (int)$left) * 1e6)) > 0) {
            $chunk = fread($s, 8192);
            if (($chunk === '' || $chunk === false) && feof($s)) return $buf;
            $buf .= (string)$chunk;
        }
    }
    return $buf === '' ? null : $buf;
}

/** Wait path for a team: /_mcp_wait/<hash>/<checked ms>/<deadline s>/<hop> */
function waitPath(string $team, ?int $checkedMs = null, ?int $deadline = null, int $hop = 1): string {
    $checkedMs ??= (int)floor(microtime(true) * 1000);
    $deadline ??= time() + 20;
    return '/_mcp_wait/' . WaitServer::teamHash($team) . "/$checkedMs/$deadline/$hop";
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
check(MsgServer::parseTeamId(' Team_4b ') === 'Team_4b', 'parseTeamId trims (client sends "subscribe: <id>")');
check(MsgServer::forLog("a\nb\r\x00c") === 'a?b??c', 'forLog replaces line breaks and control characters');
check(MsgServer::forLog(str_repeat('x', 500)) === str_repeat('x', 120) . '...', 'forLog shortens long input');
check(MsgServer::teamForLog('test') === '9f86d081', 'teamForLog: first 8 hex of sha256 (B-159)');
check(MsgServer::messageForLog('subscribe: Team_4b') === 'subscribe:' . MsgServer::teamForLog('Team_4b'), 'messageForLog hashes the subscribed team (B-159)');
check(MsgServer::messageForLog("subscribe:a b") === 'subscribe:(invalid)', 'messageForLog: invalid team id not logged');
check(MsgServer::messageForLog('push:secretTeam') === 'push:...', 'messageForLog: unknown messages only up to the first colon');
check(MsgServer::messageForLog("hello\nFAKE") === 'hello?FAKE', 'messageForLog: no line breaks');

echo "parseReact (B-110)\n";
foreach (['cake', 'heart', 'clap', 'party', 'think', 'thumbs', 'fish', 'coffee', 'bomb', 'a', 'abcdefghijkl'] as $code) {
    check(MsgServer::parseReact("react:12:345:$code") === "react:12:345:$code", "any lower-case code '$code' is forwarded");
}
check(MsgServer::parseReact("react:1:2:heart\n") === 'react:1:2:heart', 'trailing newline is trimmed');
check(MsgServer::parseReact('react:1234567890:1:clap') === 'react:1234567890:1:clap', '10 digits accepted');
check(MsgServer::parseReact('react:12345678901:1:clap') === null, '11 digits rejected');
check(MsgServer::parseReact('react::1:clap') === null, 'empty from rejected');
check(MsgServer::parseReact('react:1::clap') === null, 'empty to rejected');
check(MsgServer::parseReact('react:-1:2:clap') === null, 'sign rejected');
check(MsgServer::parseReact('react:a1:2:clap') === null, 'non-digit mkey rejected');
check(MsgServer::parseReact('react:1:2:abcdefghijklm') === null, 'code longer than 12 letters rejected');
check(MsgServer::parseReact('react:1:2:pull') === null, 'code "pull" rejected (old clients)');
check(MsgServer::parseReact('react:1:2:xpullx') === null, 'code containing "pull" rejected (old clients)');
check(MsgServer::parseReact('react:1:2:heart2') === null, 'digit in code rejected');
check(MsgServer::parseReact('react:1:2:thumbs_up') === null, 'underscore in code rejected');
check(MsgServer::parseReact('react:1:2:th-umb') === null, 'hyphen in code rejected');
check(MsgServer::parseReact("react:1:2:caf\u{e9}") === null, 'non-ASCII letter in code rejected');
check(MsgServer::parseReact('react:0:345:nudge') === 'react:0:345:nudge', 'from 0 accepted (anonymous nudge)');
check(MsgServer::parseReact('react:12:345:nudge') === 'react:12:345:nudge', 'from is forwarded unchanged (no meaning attached)');
check(MsgServer::parseReact('react:0:345:heart') === 'react:0:345:heart', 'from 0 accepted for other codes');
check(MsgServer::parseReact('react:12:0:fish') === 'react:12:0:fish', 'to 0 accepted');
check(MsgServer::parseReact('react:1:2:Heart') === null, 'upper-case letter in code rejected');
check(MsgServer::parseReact('react:1:2:heart:x') === null, 'extra field rejected');
check(MsgServer::parseReact('react:1:2') === null, 'missing code rejected');
check(MsgServer::parseReact("react:1:2:heart\nFAKE") === null, 'line break inside rejected');
check(MsgServer::parseReact('react: 1:2:heart') === null, 'whitespace inside rejected');

echo "reaction rate limit (B-131)\n";
// Simulates a sender trying a reaction every $step seconds for $duration seconds; returns forwarded count and drop labels.
$simulate = function (float $step, float $duration, float $start = 0.0, array $times = []): array {
    $ok = 0; $hits = [];
    for ($now = $start; $now < $start + $duration - 1e-9; $now += $step) {
        $times = MsgServer::pruneReactTimes($times, $now);
        $hit = MsgServer::reactLimitHit($times, $now);
        if ($hit === null) { $times[] = $now; $ok++; } else { $hits[$hit] = true; }
    }
    return [$ok, array_keys($hits), $times];
};
check(MsgServer::REACT_LIMITS === [1 => 3, 60 => 10], 'limits 3/s and 10/min');
check(MsgServer::reactLimitHit([], 0.0) === null, 'first reaction is allowed');
check(MsgServer::reactLimitHit([10.0, 10.2], 10.5) === null, 'third reaction within 1 s is allowed');
check(MsgServer::reactLimitHit([10.0, 10.2, 10.4], 10.5) === '1s', 'fourth reaction within 1 s is dropped (1s)');
check(MsgServer::reactLimitHit([10.0, 10.2, 10.4], 11.0) === null, 'reaction after exactly 1 s is allowed');
[$ok, $hits] = $simulate(0.1, 1.0);
check($ok === 3 && $hits === ['1s'], "10 tries in 1 s: 3 forwarded (got $ok)");
[$ok, $hits] = $simulate(0.1, 60.0);
check($ok === 10 && $hits === ['1s', '1m'], "10 tries per second for 1 min: 10 forwarded, the 11th dropped (got $ok)");
[$ok, $hits] = $simulate(0.4, 60.0);
check($ok === 10 && $hits === ['1m'], "a try every 0.4 s (below 3/s) for 1 min: 10 forwarded (got $ok)");
[$ok, $hits] = $simulate(1.0, 60.0);
check($ok === 10 && $hits === ['1m'], "1 try per second for 1 min: 10 forwarded (got $ok)");
[$ok] = $simulate(1.0, 61.0);
check($ok === 11, "minute window slides: 11th reaction after 60 s (got $ok)");
[$ok] = $simulate(1.0, 900.0);
check($ok === 150, "1 try per second for 15 min: 150 forwarded (10 per minute, no longer windows, got $ok)");
[$ok] = $simulate(6.0, 600.0);
check($ok === 100, "one try every 6 s for 10 min is never dropped (got $ok)");
[$ok, $hits, $times] = $simulate(0.25, 3600.0);
check(count($times) <= 10, 'state per connection stays at most 10 times (got ' . count($times) . ')');
check(MsgServer::pruneReactTimes([1.0, 2.0], 100.0) === [], 'times older than 1 min are pruned');
check(MsgServer::reactLimitHit([0.0, 1.0], 2.0, [1 => 1, 60 => 2]) === '1m', 'limits are configurable (array window => max)');

echo "daily quota per emoji (B-139)\n";
check(MsgServer::REACT_DAILY_QUOTA === 1 && MsgServer::REACT_QUOTA_EXEMPT === ['nudge'], 'quota 1 per code and day, nudge exempt');
ob_start(); $q = new MsgServer(); ob_end_clean();
$day1 = (float)gmmktime(10, 0, 0, 11, 16, 2026);
check($q->reactQuotaTake('teamA', '11', 'heart', $day1), 'first throw of a code is allowed');
check(!$q->reactQuotaTake('teamA', '11', 'heart', $day1 + 5), 'same code by the same thrower on the same day is dropped');
check($q->reactQuotaTake('teamA', '11', 'cake', $day1 + 6), 'another code by the same thrower is allowed');
check($q->reactQuotaTake('teamA', '12', 'heart', $day1 + 7), 'same code by another thrower is allowed');
check($q->reactQuotaTake('teamB', '11', 'heart', $day1 + 8), 'same mkey in another team counts separately');
$nudges = 0;
for ($i = 0; $i < 5; $i++) if ($q->reactQuotaTake('teamA', '0', 'nudge', $day1 + 10 + $i)) $nudges++;
check($nudges === 5, "nudge is exempt from the quota (got $nudges of 5)");
check($q->reactQuotaSize() === 4, 'nudges are not counted (state ' . $q->reactQuotaSize() . ' entries)');
$endOfDay = (float)gmmktime(23, 59, 59, 11, 16, 2026);
check(!$q->reactQuotaTake('teamA', '11', 'heart', $endOfDay), 'still dropped at 23:59:59 UTC');
$day2 = (float)gmmktime(0, 0, 0, 11, 17, 2026);
check($q->reactQuotaTake('teamA', '11', 'heart', $day2), 'allowed again from 00:00 UTC of the next day');
check($q->reactQuotaSize() === 1, 'counts of the previous day are dropped on the first throw of a new day');
check(!$q->reactQuotaTake('teamA', '11', 'heart', $day2 + 60), 'quota applies again on the new day');

echo "notifications for teams without subscribers\n";
$srv = new MsgServer();
ob_start();
for ($i = 0; $i < 50; $i++) $srv->onNotify("pull:ghost$i");
$out = (string)ob_get_clean();
check(substr_count($out, "\n") === 1 && substr_count($out, 'teams without subscribers') === 1,
    'no log line per notification (1 line for 50 notifications)');
check(!str_contains($out, 'New notification'), 'no pull logged for teams without subscribers');
check($srv->notifyStateSize() === 0, 'no state kept for teams without subscribers');

echo "MCP wait endpoint: parsing (B-151)\n";
check(WaitServer::teamHash('Team_4b') === substr(hash('sha256', 'Team_4b'), 0, 32), 'team hash = first 32 hex characters of sha256(team id)');
$h = WaitServer::teamHash('Team_4b');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/1") === [$h, '1764190000123', '1764190025', '1'], 'valid wait path');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/3") !== null, 'hop 3 accepted');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/4") === null, 'hop 4 rejected');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/0") === null, 'hop 0 rejected');
check(WaitServer::parsePath("/_mcp_wait/" . strtoupper($h) . "/1764190000123/1764190025/1") === null, 'upper-case hash rejected');
check(WaitServer::parsePath("/_mcp_wait/{$h}0/1764190000123/1764190025/1") === null, '33 hex characters rejected');
check(WaitServer::parsePath("/_mcp_wait/$h/176419000012/1764190025/1") === null, 'checked with 12 digits rejected');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/176419002/1") === null, 'deadline with 9 digits rejected');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/1/") === null, 'trailing slash rejected');
check(WaitServer::parsePath("/_mcp_wait/$h/1764190000123/1764190025/1\n") === null, 'trailing newline rejected');
check(WaitServer::parsePath("/_mcp_result/$h/1764190000123/1764190025/1") === null, 'other prefix rejected');
check(WaitServer::resultResponse('1764190025', '2', 'pull') === "HTTP/1.1 200 OK\r\nX-Accel-Redirect: /_mcp_result/1764190025/2?r=pull\r\n"
    . "Content-Length: 0\r\nConnection: close\r\n\r\n", 'response: 200, X-Accel-Redirect to /_mcp_result/<deadline>/<hop>?r=..., empty body');

echo "MCP wait endpoint: robustness (B-151)\n";
// an \Error inside onOpen must not leave the endpoint (IoServer only catches \Exception, it would end the loop)
$fakeConn = new class implements Ratchet\ConnectionInterface {
    public int $resourceId = 7; public bool $closed = false;
    public function send($data) { throw new \Error('boom /_mcp_wait/secret'); }
    public function close() { $this->closed = true; }
};
$errLog = [];
$ws = new WaitServer(React\EventLoop\Factory::create(), 10, function ($m) use (&$errLog) { $errLog[] = $m; });
$thrown = false;
try {
    $ws->onOpen($fakeConn, new GuzzleHttp\Psr7\Request('GET', waitPath('teamE', null, time() - 1)));
} catch (\Throwable $e) { $thrown = true; }
check(!$thrown && $fakeConn->closed && $ws->waiterCount() === 0, 'Error in onOpen is caught, connection closed, no waiter left');
check(count($errLog) === 1 && str_starts_with($errLog[0], 'ERROR: mcp request failed: Error in ')
    && !str_contains($errLog[0], '_mcp_wait') && !str_contains($errLog[0], WaitServer::teamHash('teamE')), 'error logged without message, URI or hash');

// race memory: notification times are kept PULL_MEMORY (10 s), older ones are pruned
check(WaitServer::PULL_MEMORY === 10.0, 'pull times are kept 10 s');
$ws = new WaitServer(React\EventLoop\Factory::create(), 10, fn($m) => null);
$t0 = 1764190000.0;
for ($i = 0; $i < 100; $i++) $ws->notePull("flood$i", $t0);
check($ws->pullStateSize() === 100, 'pull times of 100 teams kept');
$ws->notePull('later', $t0 + 9.0);
check($ws->pullStateSize() === 101, 'still kept after 9 s');
$ws->notePull('later', $t0 + 10.5);
check($ws->pullStateSize() === 1, 'pruned after 10 s (UDP flood does not grow the map)');

echo "MCP react endpoint: parsing and rules (B-151)\n";
$h = WaitServer::teamHash('teamM');
check(WaitServer::parseReactPath("/_mcp_react/$h/12/345/heart") === [$h, '12', '345', 'heart'], 'valid react path');
check(WaitServer::parseReactPath("/_mcp_react/$h/0/1234567890/nudge") !== null, 'from 0 and 10-digit mkeys accepted');
foreach (["/_mcp_react/" . strtoupper($h) . "/12/345/heart" => 'upper-case hash', "/_mcp_react/{$h}0/12/345/heart" => '33 hex characters',
          "/_mcp_react/$h/12345678901/345/heart" => '11-digit from', "/_mcp_react/$h/12//heart" => 'empty to',
          "/_mcp_react/$h/12/345/Heart" => 'upper-case code', "/_mcp_react/$h/12/345/abcdefghijklm" => '13-letter code',
          "/_mcp_react/$h/12/345/heart/" => 'trailing slash', "/_mcp_react/$h/12/345/heart\n" => 'trailing newline',
          "/_mcp_react/$h/12/345/heart/x" => 'extra segment', "/_mcp_react/$h/-1/345/heart" => 'sign',
          "/_mcp_wait/$h/12/345/heart" => 'other prefix'] as $bad => $what) {
    check(WaitServer::parseReactPath($bad) === null, "react path rejected: $what");
}
check(WaitServer::reactResponse('ok', ['cake', 'heart']) === "HTTP/1.1 200 OK\r\nX-Accel-Redirect: /_mcp_result/react?r=ok&used=cake,heart\r\n"
    . "Content-Length: 0\r\nConnection: close\r\n\r\n", 'react response: 200, X-Accel-Redirect to /_mcp_result/react?r=...&used=..., empty body');
check(str_contains(WaitServer::reactResponse('rate', []), "/_mcp_result/react?r=rate&used=\r\n"), 'react response: used empty if nothing thrown today');

ob_start(); $m = new MsgServer(); ob_end_clean();
check($m->reactFromMcp($h, '11', '22', 'heart') === ['ok', ['heart']], 'MCP reaction: ok, used lists the code');
check($m->reactFromMcp($h, '11', '22', 'cake') === ['ok', ['cake', 'heart']], 'MCP reaction: another code ok, used sorted');
check($m->reactFromMcp($h, '11', '33', 'heart') === ['quota', ['cake', 'heart']], 'MCP reaction: same code again (other recipient) -> quota');
check($m->reactFromMcp($h, '12', '22', 'heart') === ['ok', ['heart']], 'MCP reaction: other thrower has its own quota and rate limit');
check($m->reactFromMcp(WaitServer::teamHash('teamN'), '11', '22', 'heart') === ['ok', ['heart']], 'MCP reaction: same mkey in another team counts separately');
check($m->reactFromMcp($h, '11', '22', 'party') === ['ok', ['cake', 'heart', 'party']], 'MCP reaction: the quota drop did not count for the rate limit');
check($m->reactFromMcp($h, '11', '22', 'nudge') === ['rate', ['cake', 'heart', 'party']], 'MCP reaction: 4th forwarded try within 1 s -> rate');
check($m->reactFromMcp($h, '13', '22', 'nudge') === ['ok', []] && $m->reactFromMcp($h, '13', '22', 'nudge') === ['ok', []],
    'MCP nudge: exempt from the quota, not listed in used');
check($m->reactFromMcp($h, '11', '22', 'pull') === ['invalid', []] && $m->reactFromMcp($h, '11', '22', 'xpullx') === ['invalid', []], 'MCP reaction: code containing "pull" invalid');
check($m->reactFromMcp('nothex', '11', '22', 'heart') === ['invalid', []], 'MCP reaction: invalid hash');
check($m->reactQuotaTake($h, '14', 'think', microtime(true)) && $m->reactFromMcp($h, '14', '22', 'think') === ['quota', ['think']],
    'quota is shared with WebSocket throws of the same thrower (key: team hash)');
check($m->mcpReactStateSize() === 4, 'MCP rate state per team hash and thrower (got ' . $m->mcpReactStateSize() . ')');

// an \Error in the react handler must not leave the endpoint either
$fakeConn = new class implements Ratchet\ConnectionInterface {
    public int $resourceId = 8; public bool $closed = false; public string $sent = '';
    public function send($data) { $this->sent .= $data; }
    public function close() { $this->closed = true; }
};
$errLog = [];
$ws = new WaitServer(React\EventLoop\Factory::create(), 10, function ($m) use (&$errLog) { $errLog[] = $m; });
$ws->setReactHandler(function () { throw new \Error('boom'); });
$thrown = false;
try {
    $ws->onOpen($fakeConn, new GuzzleHttp\Psr7\Request('GET', "/_mcp_react/$h/11/22/heart"));
} catch (\Throwable $e) { $thrown = true; }
check(!$thrown && $fakeConn->closed && $fakeConn->sent === '', 'Error in the react handler is caught, connection closed without redirect');
check(count($errLog) === 1 && str_starts_with($errLog[0], 'ERROR: mcp request failed: Error in ') && !str_contains($errLog[0], $h),
    'react error logged without message, URI or hash');
$fakeConn->closed = false;
$ws2 = new WaitServer(React\EventLoop\Factory::create(), 10, fn($m) => null);
$ws2->onOpen($fakeConn, new GuzzleHttp\Psr7\Request('GET', "/_mcp_react/$h/11/22/heart"));
check($fakeConn->closed && str_starts_with($fakeConn->sent, 'HTTP/1.1 400'), 'without react handler: 400');

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

    // B-93: the browser message "push" is ignored (no pull to anyone), the connection stays usable
    $a1->send('push');
    $ra1 = $a1->receive(0.4); $ra2 = $a2->receive(0.1); $rb1 = $b1->receive(0.1);
    check(!in_array('pull', $ra1, true) && !in_array('pull', $ra2, true), 'browser push no longer triggers a pull (B-93)');
    check(!in_array('pull', $rb1, true), 'browser push does not reach other teams');
    usleep(150000);
    udp(NOTIFY_PORT, 'pull:teamA');
    check(in_array('pull', $a1->receive(0.5), true), 'UDP notification still works after an ignored push');
    $a2->receive(0.1);

    // exclusive bind: another local socket must not get the port (B-81, needs ext-sockets)
    if (function_exists('socket_create')) {
        $second = @stream_socket_server('udp://127.0.0.1:' . NOTIFY_PORT, $errno, $errstr, STREAM_SERVER_BIND);
        check($second === false, 'second bind with SO_REUSEADDR on the notify port fails');
        $sock = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
        check(@socket_bind($sock, '127.0.0.1', NOTIFY_PORT) === false, 'second plain bind on the notify port fails');
        socket_close($sock);
    } else {
        echo "  skip exclusive bind (sockets extension missing)\n";
    }

    // several notifications for one team within 100 ms: one pull now, one trailing pull
    usleep(150000);
    for ($i = 0; $i < 5; $i++) udp(NOTIFY_PORT, 'pull:teamA');
    $burst = count(array_filter($a1->receive(0.5), fn($m) => $m === 'pull'));
    check($burst === 2, "5 notifications within 100 ms give 2 pulls (got $burst)");
    $a2->receive(0.05); $b1->receive(0.05);

    // WebSocket messages: unknown messages are ignored, validated subscriptions
    $a1->send("push\nFAKE LOG LINE");
    $a1->send('xpush');
    check(!in_array('pull', $a2->receive(0.4), true), 'unknown messages trigger no pull');
    $bad = new WsClient(WS_PORT);
    $bad->send('subscribe: ' . str_repeat('a', 81));
    usleep(100000);
    udp(NOTIFY_PORT, 'pull:' . str_repeat('a', 81));
    check(!in_array('pull', $bad->receive(0.4), true), 'subscription with invalid team id is ignored');
    $bad->close();
    $nospace = subscribed(WS_PORT, 'teamD');
    $nospace->send('subscribe:teamD');
    usleep(100000);
    udp(NOTIFY_PORT, 'pull:teamD');
    check(in_array('pull', $nospace->receive(0.4), true), 'subscribe without space still works');
    $nospace->close();

    // B-110: reactions go to every connection of the team (incl. sender), not to other teams
    $isReact = fn(array $msgs) => array_values(array_filter($msgs, fn($m) => str_starts_with($m, 'react:')));
    foreach ([$a1, $a2, $b1] as $c) $c->receive(0.05);
    $a1->send('react:11:22:cake');
    $ra1 = $isReact($a1->receive(0.4)); $ra2 = $isReact($a2->receive(0.1)); $rb1 = $isReact($b1->receive(0.1));
    check($ra1 === ['react:11:22:cake'], 'reaction echoed to the sender');
    check($ra2 === ['react:11:22:cake'], 'reaction forwarded to the team');
    check($rb1 === [], 'reaction not forwarded to other teams');

    // rate limit: 3 per second (cake + heart + clap), the 4th within 1 s is dropped, afterwards accepted again
    $a1->send('react:11:22:heart');
    $a1->send('react:11:22:clap');
    $a1->send('react:11:22:wave');
    check($isReact($a2->receive(0.4)) === ['react:11:22:heart', 'react:11:22:clap'], 'the 4th reaction within 1 s is dropped');
    $b1->send('react:5:6:party');
    check($isReact($b1->receive(0.4)) === ['react:5:6:party'], 'rate limit is per connection');
    usleep(700000);
    $a1->send('react:11:22:nudge');
    check($isReact($a2->receive(0.4)) === ['react:11:22:nudge'], 'reaction accepted again after 1 s');
    $a1->receive(0.05);

    // B-139: daily quota per thrower (from-mkey) across connections; the rate-limited 'wave' was not counted
    usleep(1100000);
    $a3 = subscribed(WS_PORT, 'teamA');
    usleep(100000);
    foreach ([$a1, $a2] as $c) $c->receive(0.05);
    $a3->send('react:11:22:cake');
    check($isReact($a2->receive(0.4)) === [], 'same code by the same thrower from another connection is dropped (daily quota)');
    $a3->send('react:11:22:wave');
    check($isReact($a2->receive(0.4)) === ['react:11:22:wave'], 'code dropped by the rate limit before is not counted for the quota');
    $a3->send('react:11:22:nudge');
    check($isReact($a2->receive(0.4)) === ['react:11:22:nudge'], 'nudge is not limited by the daily quota');
    $a3->receive(0.05); $a1->receive(0.05);
    $a3->close();

    // invalid reactions and unsubscribed senders are dropped (a2 has not reacted yet, so no rate limit)
    $a2->send('react:1:2:Bomb');
    $a2->send('react:1:2:pull');
    $a2->send('react:1:2:heart:x');
    $a2->send('react:12345678901:2:heart');
    $a2->send("react:1:2:heart\nFAKE LOG LINE");
    check($isReact($a1->receive(0.4)) === [], 'invalid reactions are dropped');
    $a2->send('react:3:4:bomb');
    check($isReact($a1->receive(0.4)) === ['react:3:4:bomb'], 'invalid reactions do not count for the rate limit (unknown code forwarded)');
    $a2->receive(0.05);
    $anon = new WsClient(WS_PORT);
    $anon->send('react:1:2:think');
    check($isReact($anon->receive(0.3)) === [] && $isReact($a1->receive(0.1)) === [], 'reaction from unsubscribed connection is dropped');
    $anon->close();

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
    check(substr_count($logText, 'invalid notification(s)') === 1, 'invalid datagrams logged once per minute');
    check(!preg_match('/^FAKE LOG LINE/m', $logText), 'no log injection via line breaks');
    check(substr_count($logText, 'forwarded reaction') === 8, 'forwarded reactions logged');
    check(preg_match("/ignored reaction 'cake' from \\d+ \\(daily quota\\)$/m", $logText) === 1, 'reactions above the daily quota logged with code only (B-139)');
    check(!str_contains($logText, 'react:'), 'reaction content is not logged');
    check(str_contains($logText, ".. ignored reaction 'wave' from ") && !str_contains($logText, ".. ignored reaction 'clap' from ")
        && preg_match("/ignored reaction 'wave' from \\d+ \\(rate limit 1s\\)$/m", $logText) === 1, 'dropped reactions logged with code and window (B-131)');
    $teamALog = MsgServer::teamForLog('teamA');
    check(str_contains($logText, ".. forwarded reaction 'cake' to 2 connection(s) of team '$teamALog'"), 'forwarded reactions logged with code (B-131)');
    check(!str_contains($logText, '11:22') && !str_contains($logText, '5:6'), 'mkeys of reactions are not logged');
    check(str_contains($logText, "pull for team '$teamALog' (4 merged)"), 'merged notifications logged once');
    check(str_contains($logText, "is: subscribe:$teamALog") && str_contains($logText, "subscription on team '$teamALog'"), 'subscriptions logged with the hash prefix (B-159)');
    check(!str_contains($logText, 'teamA') && !str_contains($logText, 'teamB'), 'no team id in clear text in the log (B-159)');
    check(preg_match('/active_users=\d+$/m', $logText) === 1 && preg_match('/active_teams=\d+$/m', $logText) === 1, 'active_users/active_teams lines unchanged (cron script)');
    check(!str_contains($logText, 'WARNING: PHP extension sockets missing') || !function_exists('socket_create'), 'no fallback warning when sockets is available');
} finally {
    stopServer($proc);
}

echo "MCP wait endpoint (http " . HTTP_PORT . ", max 3 waiters)\n";
[$proc, $log] = startServer(WS_PORT, NOTIFY_PORT, '0', HTTP_PORT, 3);
try {
    $isPull = fn(?string $r, string $deadline, string $hop = '1') => $r !== null && str_starts_with($r, "HTTP/1.1 200 OK\r\n")
        && str_contains($r, "\r\nX-Accel-Redirect: /_mcp_result/$deadline/$hop?r=pull\r\n") && str_contains($r, "\r\nContent-Length: 0\r\n");

    // waiter of a team without any WebSocket subscriber wakes on the UDP pull for that team
    $dl = (string)(time() + 20);
    $w1 = waitOpen(HTTP_PORT, waitPath('teamW', null, (int)$dl, 2));
    usleep(150000);
    check(waitRead($w1, 0.3) === null, 'waiter is held (no answer without a pull)');
    udp(NOTIFY_PORT, 'pull:teamX');
    check(waitRead($w1, 0.3) === null, 'pull for another team does not wake the waiter');
    udp(NOTIFY_PORT, 'pull:teamW');
    check($isPull(waitRead($w1, 1.0), $dl, '2'), 'UDP pull wakes the waiter: r=pull, deadline and hop echoed (team without WebSocket subscribers)');
    fclose($w1);

    // WebSocket subscribers and waiters of the same team both get the pull
    usleep(150000);
    $ws = subscribed(WS_PORT, 'teamV');
    $w2 = waitOpen(HTTP_PORT, waitPath('teamV', null, (int)$dl));
    $w3 = waitOpen(HTTP_PORT, waitPath('teamV', null, (int)$dl));
    usleep(150000);
    udp(NOTIFY_PORT, 'pull:teamV');
    check(in_array('pull', $ws->receive(0.5), true), 'WebSocket subscriber still gets the pull');
    check($isPull(waitRead($w2, 0.5), $dl) && $isPull(waitRead($w3, 0.5), $dl), 'both waiters of the team get the pull');
    fclose($w2); fclose($w3); $ws->close();

    // race: a pull after "checked" (written while PHP was computing the view) answers at once
    $before = (int)floor(microtime(true) * 1000) - 50;
    udp(NOTIFY_PORT, 'pull:teamR');
    usleep(150000);
    $t0 = microtime(true);
    $w4 = waitOpen(HTTP_PORT, waitPath('teamR', $before, (int)$dl));
    check($isPull(waitRead($w4, 0.5), $dl) && microtime(true) - $t0 < 0.3, 'pull after checked: r=pull at once (race protection, team without subscribers)');
    fclose($w4);
    $w5 = waitOpen(HTTP_PORT, waitPath('teamR', null, (int)$dl));
    check(waitRead($w5, 0.4) === null, 'pull before checked: waiter is held');
    fclose($w5);

    // deadline: timeout at the deadline, at once if already over
    $dl2 = time() + 2;
    $t0 = microtime(true);
    $w6 = waitOpen(HTTP_PORT, waitPath('teamT', null, $dl2, 3));
    $r6 = waitRead($w6, 3.5);
    $took = microtime(true) - $t0;
    check($r6 !== null && str_contains($r6, "X-Accel-Redirect: /_mcp_result/$dl2/3?r=timeout\r\n"), 'r=timeout at the deadline');
    check($took >= $dl2 - $t0 - 0.1 && $took <= $dl2 - $t0 + 0.5, sprintf('timeout comes at the deadline (after %.2f s)', $took));
    fclose($w6);
    $past = time() - 1;
    $w7 = waitOpen(HTTP_PORT, waitPath('teamT', null, $past));
    $r7 = waitRead($w7, 0.5);
    check($r7 !== null && str_contains($r7, "X-Accel-Redirect: /_mcp_result/$past/1?r=timeout\r\n"), 'deadline already over: r=timeout at once');
    fclose($w7);

    // connections without complete headers are closed after 5 s; a complete waiter is held longer
    $dl3 = time() + 7;
    $held = waitOpen(HTTP_PORT, waitPath('teamH', null, $dl3));
    $partial = stream_socket_client('tcp://127.0.0.1:' . HTTP_PORT, $errno, $errstr, 2);
    fwrite($partial, "GET /_mcp_wait/");
    stream_set_blocking($partial, false);
    $t0 = microtime(true);
    $rp = waitRead($partial, 6.5);
    $took = microtime(true) - $t0;
    check($rp === '' && $took >= 4.5 && $took < 6.0, sprintf('incomplete headers: closed after 5 s (%.2f s)', $took));
    fclose($partial);
    $rh = waitRead($held, 3.0);
    check($rh !== null && str_contains($rh, "?r=timeout\r\n"), 'complete waiter is not closed by the header timeout (held until its deadline)');
    fclose($held);

    // invalid requests get 400 and no redirect
    foreach (['/_mcp_wait/abc/1/2/1', '/', '/_mcp_wait/' . WaitServer::teamHash('x') . '/1764190000123/1764190025/9'] as $bad) {
        $b = waitOpen(HTTP_PORT, $bad);
        $rb = waitRead($b, 0.5);
        check($rb !== null && str_starts_with($rb, 'HTTP/1.1 400') && !str_contains($rb, 'X-Accel-Redirect'), "invalid path '$bad': 400 without redirect");
        fclose($b);
    }
    $b = waitOpen(HTTP_PORT, waitPath('teamW'), 'POST');
    $rb = waitRead($b, 0.5);
    check($rb !== null && str_starts_with($rb, 'HTTP/1.1 400'), 'POST: 400');
    fclose($b);

    // global cap (3 here): the 4th concurrent waiter gets r=busy at once; aborted waiters free their place
    $ws1 = []; for ($i = 0; $i < 3; $i++) $ws1[] = waitOpen(HTTP_PORT, waitPath("teamB$i", null, (int)$dl));
    usleep(150000);
    $w8 = waitOpen(HTTP_PORT, waitPath('teamB9', null, (int)$dl, 2));
    $r8 = waitRead($w8, 0.5);
    check($r8 !== null && str_contains($r8, "X-Accel-Redirect: /_mcp_result/$dl/2?r=busy\r\n"), 'above the cap: r=busy at once');
    fclose($w8);
    fclose($ws1[0]);  // client abort: nginx closes the upstream connection
    usleep(150000);
    $w9 = waitOpen(HTTP_PORT, waitPath('teamB0', null, (int)$dl));
    check(waitRead($w9, 0.4) === null, 'after an abort the place is free again (waiter removed on close)');
    udp(NOTIFY_PORT, 'pull:teamB0');
    check($isPull(waitRead($w9, 0.5), $dl), 'new waiter of the aborted team gets the pull');
    fclose($w9); fclose($ws1[1]); fclose($ws1[2]);

    // B-151: MCP reactions, same rules as WebSocket reactions, distributed to the team's WebSocket connections
    $react = function (string $team, string $from, string $to, string $code, string $method = 'GET'): ?string {
        $s = waitOpen(HTTP_PORT, '/_mcp_react/' . WaitServer::teamHash($team) . "/$from/$to/$code", $method);
        $r = waitRead($s, 0.5);
        fclose($s);
        return $r;
    };
    $hasResult = fn(?string $r, string $q) => $r !== null && str_starts_with($r, "HTTP/1.1 200 OK\r\n")
        && str_contains($r, "\r\nX-Accel-Redirect: /_mcp_result/react?$q\r\n") && str_contains($r, "\r\nContent-Length: 0\r\n");
    $isReact = fn(array $msgs) => array_values(array_filter($msgs, fn($m) => str_starts_with($m, 'react:')));
    $m1 = subscribed(WS_PORT, 'teamM');
    $m2 = subscribed(WS_PORT, 'teamM');
    $o1 = subscribed(WS_PORT, 'teamO');
    usleep(150000);
    check($hasResult($react('teamM', '21', '22', 'cake'), 'r=ok&used=cake'), 'MCP reaction: r=ok&used=cake');
    check($isReact($m1->receive(0.4)) === ['react:21:22:cake'] && $isReact($m2->receive(0.1)) === ['react:21:22:cake'],
        'MCP reaction reaches every WebSocket connection of the team');
    check($isReact($o1->receive(0.1)) === [], 'MCP reaction does not reach other teams');
    check($hasResult($react('teamM', '21', '23', 'cake'), 'r=quota&used=cake'), 'same code again: r=quota');
    check($isReact($m1->receive(0.3)) === [], 'reaction above the quota is not distributed');
    // the thrower has its own (WebSocket) connection as well: quota shared, rate limit separate
    $m1->send('react:21:22:heart');
    check($isReact($m2->receive(0.4)) === ['react:21:22:heart'], 'WebSocket throw of the same thrower');
    check($hasResult($react('teamM', '21', '22', 'heart'), 'r=quota&used=cake,heart'), 'WebSocket and MCP share the daily quota (used lists both)');
    $m1->receive(0.1); $m2->receive(0.1);
    // rate limit per team hash and thrower: 3 per second
    $r41 = [$react('teamM', '41', '22', 'cake'), $react('teamM', '41', '22', 'heart'), $react('teamM', '41', '22', 'party'), $react('teamM', '41', '22', 'think')];
    check($hasResult($r41[0], 'r=ok&used=cake') && $hasResult($r41[1], 'r=ok&used=cake,heart') && $hasResult($r41[2], 'r=ok&used=cake,heart,party'),
        '3 MCP reactions within 1 s: r=ok');
    check($hasResult($r41[3], 'r=rate&used=cake,heart,party'), '4th MCP try within 1 s: r=rate, not counted for the quota');
    check($isReact($m2->receive(0.4)) === ['react:41:22:cake', 'react:41:22:heart', 'react:41:22:party'], 'only the 3 accepted reactions are distributed');
    $m1->receive(0.1);
    usleep(1000000);
    check($hasResult($react('teamM', '41', '22', 'nudge'), 'r=ok&used=cake,heart,party'), 'after 1 s accepted again (nudge exempt from the quota)');
    check($isReact($m2->receive(0.4)) === ['react:41:22:nudge'], 'nudge distributed with from as given by PHP');
    $m1->receive(0.1);
    check($hasResult($react('teamQ', '1', '2', 'party'), 'r=ok&used=party'), 'team without WebSocket subscribers: r=ok (counted, nobody to deliver to)');
    foreach (['/_mcp_react/abc/1/2/heart', '/_mcp_react/' . WaitServer::teamHash('teamM') . '/1/2/pull',
              '/_mcp_react/' . WaitServer::teamHash('teamM') . '/1/2/heart/x', '/_mcp_react/' . WaitServer::teamHash('teamM') . '/1/2/Heart',
              '/_mcp_react/'] as $bad) {
        $b = waitOpen(HTTP_PORT, $bad);
        $rb = waitRead($b, 0.5);
        check($rb !== null && str_starts_with($rb, 'HTTP/1.1 400') && !str_contains($rb, 'X-Accel-Redirect'), "invalid react path '$bad': 400 without redirect");
        fclose($b);
    }
    $rp = $react('teamM', '31', '22', 'think', 'POST');
    check($rp !== null && str_starts_with($rp, 'HTTP/1.1 400'), 'react via POST: 400');
    check($isReact($m1->receive(0.3)) === [], 'invalid react requests are not distributed');
    check($hasResult($react('teamM', '31', '22', 'think'), 'r=ok&used=think'), 'endpoint still works after invalid requests');
    $m1->close(); $m2->close(); $o1->close();

    usleep(200000);
    $logText = file_get_contents($log);
    check(str_contains($logText, 'MCP wait endpoint listening on http://127.0.0.1:' . HTTP_PORT . ' (max 3 waiters)'), 'wait endpoint logged at start');
    check(!str_contains($logText, '_mcp_wait') && !str_contains($logText, WaitServer::teamHash('teamW')), 'no wait URIs or team hashes in the log');
    check(!str_contains($logText, '_mcp_react') && !str_contains($logText, WaitServer::teamHash('teamM'))
        && substr_count($logText, 'forwarded reaction') === 1 && !str_contains($logText, '21:22'), 'MCP reactions: no URIs, hashes, mkeys or per-reaction lines in the log (only the WebSocket throw)');
    check(!str_contains($logText, 'ERROR'), 'no errors logged');
} finally {
    stopServer($proc);
}

echo "MCP wait endpoint disabled (POKER_MSG_HTTP_PORT=0)\n";
[$proc, $log] = startServer(WS_PORT, NOTIFY_PORT, '0', 0);
try {
    check(@stream_socket_client('tcp://127.0.0.1:' . HTTP_PORT, $errno, $errstr, 0.5) === false, 'no listener on the wait port');
    check(str_contains(file_get_contents($log), 'MCP wait endpoint disabled'), 'disabled endpoint is logged');
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
$httpBlocker = stream_socket_server('tcp://127.0.0.1:18448');   // B-151: wait port in use as well
[$proc, $log] = startServer(18445, 18446, '0', 18448);
try {
    $c1 = subscribed(18445, 'teamC');
    $c2 = subscribed(18445, 'teamC');
    usleep(200000);
    $c1->send('react:1:2:cake');
    check(in_array('react:1:2:cake', $c2->receive(0.5), true), 'WebSocket server keeps working without notify channel');
    check(str_contains(file_get_contents($log), 'ERROR: notify channel udp://127.0.0.1:18446 not available'), 'bind error is logged');
    check(str_contains(file_get_contents($log), 'ERROR: MCP wait endpoint 127.0.0.1:18448 not available'), 'wait endpoint bind error is logged, server keeps running (B-151)');
    $c1->close(); $c2->close();
} finally {
    stopServer($proc);
    socket_close($blocker);
    fclose($httpBlocker);
}

echo $failures === 0 ? "ALL PASSED\n" : "$failures FAILED\n";
exit($failures === 0 ? 0 : 1);
