<?php
namespace PokerMsg;
use DateTime;
use Exception;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;

class MsgServer implements MessageComponentInterface {
    protected $users;
    protected $user_cons;
    protected $user_subs;

    public function __construct() {
        $this->logMsg("Starting up MsgServer");
        $this->users = Array();
        $this->user_cons = Array();
        $this->user_subs = Array();
    }

    public function onOpen(ConnectionInterface $conn) {
        $this->users[$conn->resourceId] = $conn;
        $this->logMsg("New connection from $conn->resourceId established.");
        $this->logActiveUsers();
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        $msg = trim($msg);
        $this->logMsg("New message from $from->resourceId is: $msg");
        if (str_contains($msg, 'subscribe:')) {
            $team = trim(str_replace('subscribe:','', $msg));
            if (strlen($team)>0) {
                $this->logMsg(".. added subscription on team '$team' for $from->resourceId");
                $this->user_cons[$from->resourceId] = $from;
                $this->user_subs[$from->resourceId] = $team;
                $this->logActiveTeams();
            }
        } else if (str_contains($msg, 'push')) {

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
            $this->logMsg("Ignored invalid notification (" . strlen($data) . " bytes)");
            return;
        }
        $this->logMsg("New notification: pull for team '$team'");
        $this->pullTeam($team);
    }

    /**
     * Returns the team id of a valid "pull:<team_id>" message, otherwise null.
     * Team ids are at most 80 printable ASCII characters without whitespace (pok_teams_tbl.id is varchar(80)).
     */
    public static function parseNotify(string $data): ?string {
        $data = trim($data);
        if (!str_starts_with($data, 'pull:')) return null;
        $team = trim(substr($data, 5));
        if (preg_match('/^[\x21-\x7E]{1,80}$/', $team) !== 1) return null;
        return $team;
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
        $this->logMsg("An error has occurred: {$e->getMessage()}");
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