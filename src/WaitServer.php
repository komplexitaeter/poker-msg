<?php
namespace PokerMsg;

use Exception;
use Psr\Http\Message\RequestInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Http\HttpServerInterface;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;

/**
 * B-151: internal HTTP wait endpoint for the MCP tool wait_for_change (docs/mcp-concept.md, section 8, in the
 * poker repo). Bound to 127.0.0.1 only; nginx proxies the internal location /_mcp_wait/ to it after PHP answered
 * with "X-Accel-Redirect: /_mcp_wait/<team-hash>/<checked>/<deadline>/<hop>".
 *
 * The request is held until the next pull for that team, the deadline, or the client going away, and then
 * answered with an empty 200 and "X-Accel-Redirect: /_mcp_result/<deadline>/<hop>?r=pull|timeout|busy", which
 * hands the request back to PHP. poker-msg sees no body, no headers that matter and no team id, only the hash.
 */
final class WaitServer implements HttpServerInterface {
    /** "/_mcp_wait/<team-hash>/<checked ms>/<deadline s>/<hop>"; nothing else is accepted */
    public const PATH_PATTERN = '#^/_mcp_wait/([0-9a-f]{32})/(\d{13})/(\d{10})/([1-3])$#D';
    /** A waiter is never held longer than this (seconds), whatever the deadline says (nginx proxy_read_timeout 35s). */
    public const MAX_HOLD = 30.0;
    /** Pull times are kept this long (seconds) for the race check; older ones cannot be after a fresh "checked". */
    public const PULL_MEMORY = 60.0;
    /** Counters are logged once per this interval (seconds), only if something happened. */
    public const LOG_INTERVAL = 60;

    private LoopInterface $loop;
    private int $maxWait;
    /** @var callable(string): void */
    private $log;

    /** @var array<int, array{conn: ConnectionInterface, hash: string, deadline: string, hop: string, timer: TimerInterface}> */
    private array $waiters = [];
    /** @var array<string, array<int, true>> team hash => resource ids of its waiters */
    private array $byHash = [];
    /** @var array<string, float> team hash => time (ms) of the last pull notification for that team */
    private array $lastPull = [];
    private float $lastPruneAt = 0.0;

    /** @var array<string, int> counters since the last log line */
    private array $stats = ['started' => 0, 'pull' => 0, 'timeout' => 0, 'busy' => 0, 'aborted' => 0, 'invalid' => 0];

    public function __construct(LoopInterface $loop, int $maxWait, callable $log) {
        $this->loop = $loop;
        $this->maxWait = $maxWait;
        $this->log = $log;
    }

    /** Team hash as used in the wait URI: first 32 hex characters of sha256(team id). */
    public static function teamHash(string $team): string {
        return substr(hash('sha256', $team), 0, 32);
    }

    /**
     * Parses a wait path; returns [hash, checked, deadline, hop] or null.
     * @return array{0: string, 1: string, 2: string, 3: string}|null
     */
    public static function parsePath(string $path): ?array {
        if (preg_match(self::PATH_PATTERN, $path, $m) !== 1) return null;
        return [$m[1], $m[2], $m[3], $m[4]];
    }

    /** Raw HTTP response handing the request back to PHP via nginx. */
    public static function resultResponse(string $deadline, string $hop, string $reason): string {
        return "HTTP/1.1 200 OK\r\n"
            . "X-Accel-Redirect: /_mcp_result/$deadline/$hop?r=$reason\r\n"
            . "Content-Length: 0\r\n"
            . "Connection: close\r\n\r\n";
    }

    /**
     * Records a pull notification for a team (called on every valid notification, also for teams without any
     * subscriber or waiter), so a waiter that arrives later with an older "checked" is answered at once.
     */
    public function notePull(string $team): void {
        $now = microtime(true);
        $this->lastPull[self::teamHash($team)] = $now * 1000;
        if ($now - $this->lastPruneAt >= 1.0) {   // keep the map small, at most once per second
            $this->lastPruneAt = $now;
            $limit = ($now - self::PULL_MEMORY) * 1000;
            $this->lastPull = array_filter($this->lastPull, fn($t) => $t >= $limit);
        }
    }

    public function hasWaiters(string $team): bool {
        return !empty($this->byHash[self::teamHash($team)]);
    }

    /** Answers every waiter of the team with r=pull; returns their number. */
    public function wake(string $team): int {
        $hash = self::teamHash($team);
        $cnt = 0;
        foreach (array_keys($this->byHash[$hash] ?? []) as $id) {
            $this->finish($id, 'pull');
            $cnt++;
        }
        return $cnt;
    }

    public function waiterCount(): int {
        return count($this->waiters);
    }

    /** Number of teams in the race-check state (for tests). */
    public function pullStateSize(): int {
        return count($this->lastPull);
    }

    public function onOpen(ConnectionInterface $conn, ?RequestInterface $request = null) {
        if ($request === null) {
            $conn->close();
            return;
        }
        $parts = $request->getMethod() === 'GET' ? self::parsePath($request->getUri()->getPath()) : null;
        if ($parts === null) {
            // Only PHP (through nginx) calls this; anything else gets no redirect. The URI is not logged.
            $this->count('invalid');
            $conn->send("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            $conn->close();
            return;
        }
        [$hash, $checked, $deadline, $hop] = $parts;

        if (count($this->waiters) >= $this->maxWait) {
            $this->count('busy');
            $this->respond($conn, $deadline, $hop, 'busy');
            return;
        }
        $this->count('started');
        // Race: a pull after PHP computed the view ("checked") must not get lost while the request travelled here.
        if (isset($this->lastPull[$hash]) && $this->lastPull[$hash] >= (float)$checked) {
            $this->count('pull');
            $this->respond($conn, $deadline, $hop, 'pull');
            return;
        }
        $hold = min(self::MAX_HOLD, max(0.0, (float)$deadline - microtime(true)));
        if ($hold <= 0.0) {
            $this->count('timeout');
            $this->respond($conn, $deadline, $hop, 'timeout');
            return;
        }
        $id = $conn->resourceId;
        $timer = $this->loop->addTimer($hold, function () use ($id) {
            // An exception in a timer callback would end the process, so catch everything here.
            try {
                $this->finish($id, 'timeout');
            } catch (\Throwable $e) {
                ($this->log)('ERROR: mcp wait timeout failed: ' . MsgServer::forLog($e->getMessage(), 300));
            }
        });
        $this->waiters[$id] = ['conn' => $conn, 'hash' => $hash, 'deadline' => $deadline, 'hop' => $hop, 'timer' => $timer];
        $this->byHash[$hash][$id] = true;
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        // A wait request has no body (nginx: proxy_pass_request_body off); extra data is ignored.
    }

    public function onClose(ConnectionInterface $conn) {
        // Still waiting means the other side went away (client abort: nginx closes the upstream connection).
        if ($this->remove($conn->resourceId)) {
            $this->count('aborted');
        }
    }

    public function onError(ConnectionInterface $conn, Exception $e) {
        if ($this->remove($conn->resourceId)) {
            $this->count('aborted');
        }
        $conn->close();
    }

    private function finish(int $id, string $reason): void {
        $w = $this->waiters[$id] ?? null;
        if ($w === null) return;
        $this->remove($id);
        $this->count($reason);
        $this->respond($w['conn'], $w['deadline'], $w['hop'], $reason);
    }

    /** Removes a waiter and its timer; returns false if there was none. */
    private function remove(int $id): bool {
        $w = $this->waiters[$id] ?? null;
        if ($w === null) return false;
        $this->loop->cancelTimer($w['timer']);
        unset($this->waiters[$id], $this->byHash[$w['hash']][$id]);
        if (empty($this->byHash[$w['hash']])) unset($this->byHash[$w['hash']]);
        return true;
    }

    private function respond(ConnectionInterface $conn, string $deadline, string $hop, string $reason): void {
        $conn->send(self::resultResponse($deadline, $hop, $reason));
        $conn->close();
    }

    private function count(string $kind): void {
        $this->stats[$kind]++;
    }

    /**
     * Logs the counters since the last call (no URIs, no hashes, no team ids); nothing if nothing happened.
     * Called by a periodic timer every LOG_INTERVAL seconds.
     */
    public function logStats(): void {
        if (array_sum($this->stats) === 0) return;
        $parts = [];
        foreach ($this->stats as $k => $v) $parts[] = "$k=$v";
        ($this->log)('mcp_wait: waiting=' . count($this->waiters) . ', ' . implode(', ', $parts));
        $this->stats = array_map(fn() => 0, $this->stats);
    }
}
