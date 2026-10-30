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

    // B-81: pulls for the same team within this window (seconds) are merged into one trailing pull
    public const NOTIFY_WINDOW = 0.1;
    // B-81: invalid notifications are logged at most once per this interval (seconds)
    public const INVALID_LOG_INTERVAL = 60;

    private ?LoopInterface $loop = null;
    /** @var array<string, float> team id => time of the last pull sent for a notification */
    private array $lastNotify = [];
    /** @var array<string, int> team id => notifications merged into the scheduled trailing pull */
    private array $pendingNotify = [];
    /** @var array<string, int> kind => notifications ignored since the last log line */
    private array $ignoredCount = [];
    /** @var array<string, float> kind => time of the last log line */
    private array $ignoredLoggedAt = [];
    private float $lastPruneAt = 0.0;

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

    public function onOpen(ConnectionInterface $conn) {
        $this->users[$conn->resourceId] = $conn;
        $this->logMsg("New connection from $conn->resourceId established.");
        $this->logActiveUsers();
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        $msg = trim((string)$msg);
        $this->logMsg("New message from $from->resourceId is: " . self::forLog($msg));
        // The client sends exactly "subscribe: <team_id>" and "push" (script.js).
        if (str_starts_with($msg, 'subscribe:')) {
            $team = self::parseTeamId(substr($msg, strlen('subscribe:')));
            if ($team === null) {
                $this->logMsg(".. ignored subscription with invalid team id from $from->resourceId");
            } else {
                $this->logMsg(".. added subscription on team '$team' for $from->resourceId");
                $this->user_cons[$from->resourceId] = $from;
                $this->user_subs[$from->resourceId] = $team;
                $this->logActiveTeams();
            }
        } else if ($msg === 'push') {

            if (isset($this->user_subs[$from->resourceId])) {
                $team = $this->user_subs[$from->resourceId];
                $this->logMsg(".. received a push notification to all users of team: $team");
                $this->pullTeam($team);
            }
        }
    }

    /**
     * Send "pull" to every connection subscribed to the given team (B-81).
     * Used by the browser "push" and by the local UDP notification channel.
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
        // Nobody is watching this team: drop early, no log line per notification, no state.
        if (!in_array($team, $this->user_subs, true)) {
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
        unset($this->user_subs[$conn->resourceId]);
        unset($this->user_cons[$conn->resourceId]);
        unset($this->users[$conn->resourceId]);
        $this->logMsg("Connection {$conn->resourceId} has disconnected.");
        $this->logActiveUsers();
        $this->logActiveTeams();
    }

    public function onError(ConnectionInterface $conn, Exception $e) {
        if (isset($conn->resourceId)) {
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