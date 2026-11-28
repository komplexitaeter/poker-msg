<?php
namespace PokerMsg;
use DateTime;
use Exception;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use React\EventLoop\LoopInterface;

class MsgServer implements MessageComponentInterface {
    protected $users;
    protected $user_cons;
    protected $user_subs;
    /** @var array<int, string> resource id => team hash of its subscription (B-151, delivery of MCP reactions) */
    private array $user_sub_hash = [];

    // B-81: pulls for the same team within this window (seconds) are merged into one trailing pull
    public const NOTIFY_WINDOW = 0.1;
    // B-81: invalid notifications are logged at most once per this interval (seconds)
    public const INVALID_LOG_INTERVAL = 60;

    private ?LoopInterface $loop = null;
    /** B-151: MCP wait endpoint; its waiters count as subscribers of their team */
    private ?WaitServer $wait = null;
    /** @var array<string, float> team id => time of the last pull sent for a notification */
    private array $lastNotify = [];
    /** @var array<string, int> team id => notifications merged into the scheduled trailing pull */
    private array $pendingNotify = [];
    /** @var array<string, int> kind => notifications ignored since the last log line */
    private array $ignoredCount = [];
    /** @var array<string, float> kind => time of the last log line */
    private array $ignoredLoggedAt = [];
    private float $lastPruneAt = 0.0;

    // B-110: reactions "react:<from-mkey>:<to-mkey>:<code>". The server checks only the format of the code
    // (REACT_CODE_PATTERN); which codes exist is decided by the board alone (it ignores unknown codes).
    // No code may contain "pull": older boards reload on any message that includes('pull').
    public const REACT_CODE_PATTERN = '[a-z]{1,12}';
    // B-131: staggered rate limit per connection (sliding windows): window in seconds => max forwarded
    // reactions within that window. Reactions above any limit are dropped.
    public const REACT_LIMITS = [1 => 3, 60 => 10];
    /** @var array<int, list<float>> resource id => times of the last forwarded reactions (oldest first) */
    private array $lastReact = [];
    // B-139: daily quota per thrower: each sender (from-mkey within its team) may throw every code at most
    // REACT_DAILY_QUOTA times per UTC day (00:00 UTC, the board uses the same boundary). Codes in
    // REACT_QUOTA_EXEMPT only have the rate limit. Counted in memory only; a restart resets the quota.
    public const REACT_DAILY_QUOTA = 1;
    public const REACT_QUOTA_EXEMPT = ['nudge'];
    /**
     * Keyed by team hash (WaitServer::teamHash), so WebSocket and MCP throws of the same person share the quota
     * (B-151: the MCP path only knows the hash).
     * @var array<string, array<string, array<string, int>>> team hash => from => code => reactions on $reactQuotaDay
     */
    private array $reactQuota = [];
    /** UTC day (Y-m-d) the counts in $reactQuota belong to */
    private string $reactQuotaDay = '';
    // B-151: MCP reactions (/_mcp_react/) have no connection; their rate limit (REACT_LIMITS) is kept per team hash
    // and thrower instead.
    /** @var array<string, list<float>> "<team hash>:<from>" => times of the last forwarded MCP reactions */
    private array $mcpReactTimes = [];
    private float $mcpReactPrunedAt = 0.0;

    public function __construct() {
        $this->logMsg("Starting up MsgServer");
        $this->users = Array();
        $this->user_cons = Array();
        $this->user_subs = Array();
    }

    /** Event loop for timers (merging notifications); without it every notification is sent at once. */
    public function setLoop(LoopInterface $loop): void {
        $this->loop = $loop;
    }

    /** B-151: every pull for a team also wakes the MCP waiters of that team. */
    public function setWaitServer(WaitServer $wait): void {
        $this->wait = $wait;
    }

    public function onOpen(ConnectionInterface $conn) {
        $this->users[$conn->resourceId] = $conn;
        $this->logMsg("New connection from $conn->resourceId established.");
        $this->logActiveUsers();
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        $msg = trim((string)$msg);
        // Reactions (B-110) are logged by onReact() with their validated code only (B-131), never the mkeys.
        if (!str_starts_with($msg, 'react:')) {
            $this->logMsg("New message from $from->resourceId is: " . self::forLog($msg));
        }
        // The client sends exactly "subscribe: <team_id>" (script.js) and "react:..." (B-110).
        // B-93: "push" is no longer supported (since B-81 the API notifies via UDP); like any other unknown
        // message it is ignored and only shows up in the "New message" log line above (without team id).
        if (str_starts_with($msg, 'subscribe:')) {
            $team = self::parseTeamId(substr($msg, strlen('subscribe:')));
            if ($team === null) {
                $this->logMsg(".. ignored subscription with invalid team id from $from->resourceId");
            } else {
                $this->logMsg(".. added subscription on team '$team' for $from->resourceId");
                $this->user_cons[$from->resourceId] = $from;
                $this->user_subs[$from->resourceId] = $team;
                $this->user_sub_hash[$from->resourceId] = WaitServer::teamHash($team);
                $this->logActiveTeams();
            }
        } else if (str_starts_with($msg, 'react:')) {
            $this->onReact($from, $msg);
        }
    }

    /**
     * Forward a reaction (B-110) unchanged to every connection of the sender's team, including the
     * sender (it animates on the echo). No state besides the per-connection rate limit and the daily quota
     * per sender (B-139, memory only). Checks in this order: format, rate limit, daily quota; invalid
     * reactions, reactions from unsubscribed connections and reactions above a limit are dropped.
     * Dropped reactions do not count for the rate limit or the quota.
     */
    private function onReact(ConnectionInterface $from, string $msg): void {
        $id = $from->resourceId;
        if (!isset($this->user_subs[$id])) {
            $this->logMsg(".. ignored reaction from unsubscribed connection $id");
            return;
        }
        $react = self::parseReact($msg);
        if ($react === null) {
            $this->logMsg(".. ignored invalid reaction from $id");
            return;
        }
        // B-131: only the validated code (REACT_CODE_PATTERN) is logged, never the mkeys
        [, $sender, , $code] = explode(':', $react);
        $team = $this->user_subs[$id];
        $hash = $this->user_sub_hash[$id];
        $times = $this->lastReact[$id] ?? [];
        $outcome = $this->reactAdmit($times, $hash, $sender, $code, microtime(true), $hit);
        $this->lastReact[$id] = $times;
        if ($outcome === 'rate') {
            $this->logMsg(".. ignored reaction '$code' from $id (rate limit $hit)");
            return;
        }
        if ($outcome === 'quota') {
            $this->logMsg(".. ignored reaction '$code' from $id (daily quota)");
            return;
        }
        $cnt = $this->deliverReact($hash, $react);
        $this->logMsg(".. forwarded reaction '$code' to $cnt connection(s) of team '$team'");
    }

    /**
     * B-151: a reaction from the MCP server (internal HTTP endpoint /_mcp_react/, docs/mcp-concept.md 8.6 in the poker
     * repo). PHP has checked the seat and set $from from the actor. Same rules as onReact (format, rate limit,
     * daily quota); the rate limit is kept per team hash and thrower, because there is no connection. On success the
     * reaction goes to every WebSocket connection subscribed to the team, exactly like a WebSocket reaction.
     * Nothing is logged here (the endpoint only counts outcomes).
     * @return array{0: string, 1: list<string>} outcome ('ok', 'rate', 'quota' or 'invalid') and the codes the
     *         thrower has thrown today in this team (sorted)
     */
    public function reactFromMcp(string $hash, string $from, string $to, string $code): array {
        $react = preg_match('/^[0-9a-f]{32}$/D', $hash) === 1 ? self::parseReact("react:$from:$to:$code") : null;
        if ($react === null) return ['invalid', []];
        [, $from, , $code] = explode(':', $react);
        $now = microtime(true);
        if ($now - $this->mcpReactPrunedAt >= 1.0) {   // keep the map small, at most once per second
            $this->mcpReactPrunedAt = $now;
            foreach ($this->mcpReactTimes as $k => $t) {
                $t = self::pruneReactTimes($t, $now);
                if ($t === []) unset($this->mcpReactTimes[$k]); else $this->mcpReactTimes[$k] = $t;
            }
        }
        $key = "$hash:$from";
        $times = $this->mcpReactTimes[$key] ?? [];
        $outcome = $this->reactAdmit($times, $hash, $from, $code, $now, $hit);
        if ($times === []) unset($this->mcpReactTimes[$key]); else $this->mcpReactTimes[$key] = $times;
        if ($outcome === 'ok') $this->deliverReact($hash, $react);
        return [$outcome, $this->reactQuotaUsed($hash, $from, $now)];
    }

    /**
     * The rules for every reaction (B-131, B-139), shared by WebSocket (onReact) and MCP (reactFromMcp): rate limit
     * over $times (the sender's forwarded reactions, oldest first; pruned and, on success, extended in place), then
     * the daily quota of thrower $from in the team. Returns 'ok', 'rate' ($hit = window label) or 'quota'.
     * Dropped reactions count for neither limit.
     */
    private function reactAdmit(array &$times, string $hash, string $from, string $code, float $now, ?string &$hit = null): string {
        $times = self::pruneReactTimes($times, $now);
        $hit = self::reactLimitHit($times, $now);
        if ($hit !== null) return 'rate';
        if (!$this->reactQuotaTake($hash, $from, $code, $now)) return 'quota';
        $times[] = $now;
        return 'ok';
    }

    /** Sends a validated reaction to every connection subscribed to the team with this hash; returns their number. */
    private function deliverReact(string $hash, string $react): int {
        $cnt = 0;
        foreach ($this->user_cons as $user) {
            if (($this->user_sub_hash[$user->resourceId] ?? null) === $hash) {
                $user->send($react);
                $cnt++;
            }
        }
        return $cnt;
    }

    /** Number of entries in the MCP rate-limit state (for tests). */
    public function mcpReactStateSize(): int {
        return count($this->mcpReactTimes);
    }

    /**
     * B-139: counts one reaction of $code by sender $from in the team (key: team hash) for the UTC day of $now and
     * returns true, or returns false (nothing counted) if the daily quota is used up. Exempt codes always return true
     * and are not counted. On the first call of a new UTC day all counts of the previous day are dropped.
     */
    public function reactQuotaTake(string $team, string $from, string $code, float $now): bool {
        if (in_array($code, self::REACT_QUOTA_EXEMPT, true)) return true;
        $this->reactQuotaRoll($now);
        $cnt = $this->reactQuota[$team][$from][$code] ?? 0;
        if ($cnt >= self::REACT_DAILY_QUOTA) return false;
        $this->reactQuota[$team][$from][$code] = $cnt + 1;
        return true;
    }

    /**
     * B-151: codes thrown today (UTC) by sender $from in the team (key: team hash), sorted. With REACT_DAILY_QUOTA = 1
     * these are exactly the codes used up for today.
     * @return list<string>
     */
    public function reactQuotaUsed(string $team, string $from, float $now): array {
        $this->reactQuotaRoll($now);
        $codes = array_keys($this->reactQuota[$team][$from] ?? []);
        sort($codes);
        return array_map('strval', $codes);
    }

    /** B-139: drops all counts when $now is on another UTC day than the counts. */
    private function reactQuotaRoll(float $now): void {
        $day = gmdate('Y-m-d', (int)floor($now));
        if ($day !== $this->reactQuotaDay) {
            $this->reactQuota = [];
            $this->reactQuotaDay = $day;
        }
    }

    /** B-139: number of entries (team, thrower, code) in the daily quota state (for tests). */
    public function reactQuotaSize(): int {
        $n = 0;
        foreach ($this->reactQuota as $byFrom) {
            foreach ($byFrom as $codes) $n += count($codes);
        }
        return $n;
    }

    /**
     * B-131: returns the label of the first exceeded window (e.g. "1s", "1m") if one more reaction
     * at $now would exceed a limit of REACT_LIMITS, otherwise null. $times: forwarded reactions, oldest first.
     */
    public static function reactLimitHit(array $times, float $now, array $limits = self::REACT_LIMITS): ?string {
        foreach ($limits as $window => $max) {
            $cnt = 0;
            foreach ($times as $t) {
                if ($now - $t < $window) $cnt++;
            }
            if ($cnt >= $max) {
                return $window % 60 === 0 ? ($window / 60) . 'm' : $window . 's';
            }
        }
        return null;
    }

    /**
     * B-131: keeps only the reaction times still relevant for REACT_LIMITS (inside the largest window and at
     * most as many as its maximum), so the state per connection stays small (at most 10 floats).
     * @return list<float>
     */
    public static function pruneReactTimes(array $times, float $now, array $limits = self::REACT_LIMITS): array {
        $window = (int)max(array_keys($limits));
        $max = (int)max($limits);
        $times = array_values(array_filter($times, fn($t) => $now - $t < $window));
        return count($times) > $max ? array_slice($times, -$max) : $times;
    }

    /**
     * Returns the normalized message of a valid reaction "react:<from-mkey>:<to-mkey>:<code>", otherwise null.
     * mkeys: 1-10 digits ("0" allowed); code: 1-12 lower-case letters a-z (ASCII), must not contain "pull".
     */
    public static function parseReact(string $msg): ?string {
        if (preg_match('/^react:([0-9]{1,10}):([0-9]{1,10}):(' . self::REACT_CODE_PATTERN . ')$/D', trim($msg), $m) !== 1) return null;
        if (str_contains($m[3], 'pull')) return null;
        return "react:{$m[1]}:{$m[2]}:{$m[3]}";
    }

    /**
     * Send "pull" to every connection subscribed to the given team (B-81).
     * Used by the local UDP notification channel (onNotify).
     */
    public function pullTeam(string $team): int {
        $cnt = 0;
        foreach ($this->user_cons as $user) {
            if (isset($this->user_subs[$user->resourceId]) and $this->user_subs[$user->resourceId] == $team) {
                $this->logMsg(".. posting pull request to $user->resourceId");
                $user->send("pull");
                $cnt++;
            }
        }
        return $cnt;
    }

    /**
     * Handle a datagram from the local notification channel (B-81).
     * Format: "pull:<team_id>"; anything else is ignored.
     */
    public function onNotify(string $data): void {
        $team = self::parseNotify($data);
        if ($team === null) {
            $this->countIgnored('invalid notification(s)', 'last one ' . strlen($data) . ' bytes');
            return;
        }
        // B-151: remember the time for every team (race check of MCP waiters that are just on their way here).
        $this->wait?->notePull($team);
        // Nobody is watching this team (no WebSocket subscriber, no MCP waiter): drop early,
        // no log line per notification, no further state.
        if (!in_array($team, $this->user_subs, true) && !($this->wait?->hasWaiters($team))) {
            $this->countIgnored('notification(s) for teams without subscribers', "last one for '$team'");
            return;
        }

        if (isset($this->pendingNotify[$team])) {
            $this->pendingNotify[$team]++;     // a trailing pull is already scheduled
            return;
        }
        $now = microtime(true);
        $since = $now - ($this->lastNotify[$team] ?? 0.0);
        if ($this->loop === null || $since >= self::NOTIFY_WINDOW) {
            $this->sendNotifyPull($team, 1);
            return;
        }
        // Same team again within the window: one pull at the end of the window. It must not be
        // dropped, because the earlier pull may have been read before this change was written.
        $this->pendingNotify[$team] = 1;
        $this->loop->addTimer(self::NOTIFY_WINDOW - $since, function () use ($team) {
            // An exception in a timer callback would end the process, so catch everything here.
            try {
                $cnt = $this->pendingNotify[$team] ?? 1;
                unset($this->pendingNotify[$team]);
                $this->sendNotifyPull($team, $cnt);
            } catch (\Throwable $e) {
                $this->logMsg('ERROR: ' . self::forLog($e->getMessage(), 300));
            }
        });
    }

    /** Counts an ignored notification and logs the count at most once per INVALID_LOG_INTERVAL. */
    private function countIgnored(string $kind, string $detail): void {
        $this->ignoredCount[$kind] = ($this->ignoredCount[$kind] ?? 0) + 1;
        $now = microtime(true);
        if ($now - ($this->ignoredLoggedAt[$kind] ?? 0.0) >= self::INVALID_LOG_INTERVAL) {
            $this->logMsg("Ignored {$this->ignoredCount[$kind]} $kind, $detail");
            $this->ignoredCount[$kind] = 0;
            $this->ignoredLoggedAt[$kind] = $now;
        }
    }

    /** Number of teams in the notification state (for tests). */
    public function notifyStateSize(): int {
        return count($this->lastNotify) + count($this->pendingNotify);
    }

    private function sendNotifyPull(string $team, int $merged): void {
        $now = microtime(true);
        $this->lastNotify[$team] = $now;
        if ($now - $this->lastPruneAt >= 1.0) {   // keep the map small, at most once per second
            $this->lastPruneAt = $now;
            $this->lastNotify = array_filter($this->lastNotify, fn($t) => $now - $t < self::NOTIFY_WINDOW);
            $this->lastNotify[$team] = $now;
        }
        $this->logMsg("New notification: pull for team '$team'" . ($merged > 1 ? " ($merged merged)" : ""));
        $this->pullTeam($team);
        $this->wait?->wake($team);
    }

    /** Printable, shortened version of client input for the log (no line breaks or control characters). */
    public static function forLog(string $text, int $max = 120): string {
        $clean = preg_replace('/[^\x20-\x7E]/', '?', $text) ?? '';
        return strlen($clean) > $max ? substr($clean, 0, $max) . '...' : $clean;
    }

    /** Team id: 1-80 printable ASCII characters without whitespace (pok_teams_tbl.id is varchar(80)), else null. */
    public static function parseTeamId(string $text): ?string {
        $team = trim($text);
        return preg_match('/^[\x21-\x7E]{1,80}$/', $team) === 1 ? $team : null;
    }

    /**
     * Returns the team id of a valid "pull:<team_id>" message, otherwise null.
     */
    public static function parseNotify(string $data): ?string {
        $data = trim($data);
        if (!str_starts_with($data, 'pull:')) return null;
        return self::parseTeamId(substr($data, 5));
    }

    /**
     * Send the text message "hb" to every open connection (B-81), so clients can detect
     * a silently dead connection. Not logged per connection to keep the info log small.
     */
    public function heartbeat(): void {
        foreach ($this->users as $conn) {
            try {
                $conn->send("hb");
            } catch (\Throwable $e) {
                $this->logMsg("Heartbeat to {$conn->resourceId} failed: {$e->getMessage()}");
            }
        }
    }

    public function onClose(ConnectionInterface $conn) {
        unset($this->lastReact[$conn->resourceId]);
        unset($this->user_sub_hash[$conn->resourceId]);
        unset($this->user_subs[$conn->resourceId]);
        unset($this->user_cons[$conn->resourceId]);
        unset($this->users[$conn->resourceId]);
        $this->logMsg("Connection {$conn->resourceId} has disconnected.");
        $this->logActiveUsers();
        $this->logActiveTeams();
    }

    public function onError(ConnectionInterface $conn, Exception $e) {
        if (isset($conn->resourceId)) {
            unset($this->lastReact[$conn->resourceId]);
            unset($this->user_sub_hash[$conn->resourceId]);
            unset($this->user_subs[$conn->resourceId]);
            unset($this->user_cons[$conn->resourceId]);
            unset($this->users[$conn->resourceId]);
        }
        $this->logMsg("An error has occurred: " . self::forLog($e->getMessage(), 300));
        $conn->close();
    }

    private function logActiveUsers() {
        $num_cons = count($this->users);
        $this->logMsg("active_users=$num_cons");
    }

    private function logActiveTeams() {
        $teams = array();
        foreach ($this->user_subs as $user_sub) {
            $teams[$user_sub] = 1;
        }
        $this->logMsg("active_teams=".count($teams));
    }
    public function logMsg($msg) {
        $date = new DateTime();
        $date = $date->format("Y/m/d H:i:s");
        $delimiter = " ";
        echo $date.$delimiter.trim($msg)."\n";
    }
}