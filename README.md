# poker-msg
Poker Message-Notification System

This project provides a simple WebSocket based notification system. It ist required, to run the LAMP based application
komplexitaeter/poker. It will be run standalone on PHP-CLI and will be accessed from clientside under same protocol,
host and port as the LAMP resources. So additional configuration on webserver/proxy level ist needed, to forward ws/wss
calls to this server. Here is a simple example on nginx (to be modified based on your local system setup):

 ```
server {

    server_name localhost;
    listen 80;
    root /var/www/html/;
    index index.php;

    location ~ \.php$ {
        include snippets/fastcgi-php.conf;
        fastcgi_pass unix:/run/php/php-fpm.sock;
    }

    location /poker-msg {
        # Forward WS connections to the WS server
        proxy_pass http://127.0.0.1:8443;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
 ```

To install this project on your server you might follow this example (Linux ubuntu). It is recommended, to have the following
packages up and running: php, composer, supervisor, git. Use e.g. apt-get to install them, also refer to http://socketo.me/docs pls 
and then follow this steps:

 ```
 mkdir /home/poker/msg #create document root e.g.
 mkdir /home/poker/logs #to place some logfiles later
 git clone -b master https://github.com/komplexitaeter/poker-msg /home/poker/msg #get sources from git
 cd /home/poker/msg
 composer install #download and build the required dependencies
 sudo touch /etc/supervisor/conf.d/poker-msg.conf
 sudo vi /etc/supervisor/conf.d/poker-msg.conf #example below
 sudo systemctl restart supervisor 
 sudo systemctl enable supervisor #might be done already
 ```

The server pings every client every 30 seconds and closes connections that did not answer the previous ping
(dead tabs, sleeping laptops, connections dropped by proxies). The interval can be changed with the environment
variable `POKER_MSG_KEEPALIVE` (seconds), e.g. `environment = POKER_MSG_KEEPALIVE="30"` in the supervisor config.

### Heartbeat (B-81)

Additionally every connection receives the text message `hb` every 30 seconds. Unlike ping/pong it is visible to the
page's JavaScript, so a client can tell that its connection is still alive. Clients must ignore unknown messages
(the board only reacts to messages containing `pull`). Interval: `POKER_MSG_HEARTBEAT` (seconds, `0` disables it).
Heartbeats are not logged.

### Notifications from the web server (B-81)

Besides the WebSocket port the server listens on a local UDP port for notifications from the PHP application:

* Address: `udp://127.0.0.1:8444` (always localhost; port via `POKER_MSG_NOTIFY_PORT`).
* Format: one datagram `pull:<team_id>`, e.g. `pull:Team_4b`. The team id has at most 80 printable ASCII characters
  without whitespace; anything else is ignored. Invalid datagrams are logged at most once per minute (with a count).
* Effect: every connection subscribed to that team receives `pull`, exactly like after a `push` from the browser
  (which keeps working). Notifications for the same team within 100 ms are merged: one `pull` at once, one more at
  the end of the 100 ms window (so the last change is always loaded).
* Fire and forget: no answer, no delivery guarantee. The sender never blocks, even if poker-msg is not running.
* Security: bound to 127.0.0.1 only, but **every local process (any user) can send**. The worst a local sender can do
  is trigger additional reloads. If the port cannot be bound (e.g. in use), the error is logged and the WebSocket
  server keeps running without this channel.
* Exclusive bind needs the PHP extension `sockets` (check: `php8.4 -m | grep sockets`). With it the port is bound
  without `SO_REUSEADDR`, so no other local process can bind the same port and read the notifications. Without it
  poker-msg falls back to `stream_socket_server()` (PHP always sets `SO_REUSEADDR` there) and logs a `WARNING`.

Sending from PHP:

 ```
 $sock = @stream_socket_client('udp://127.0.0.1:8444', $errno, $errstr);
 if ($sock !== false) { @fwrite($sock, 'pull:' . $team_id); fclose($sock); }
 ```

Test on the server: `printf 'pull:test' | nc -u -w1 127.0.0.1 8444` writes `New notification: pull for team 'test'`
to the info log; `ss -ulpn | grep 8444` shows the listener.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `POKER_MSG_PORT` | `8443` | WebSocket port (127.0.0.1) |
| `POKER_MSG_KEEPALIVE` | `30` | ping interval in seconds |
| `POKER_MSG_HEARTBEAT` | `30` | `hb` interval in seconds, `0` = off |
| `POKER_MSG_NOTIFY_PORT` | `8444` | UDP notification port (127.0.0.1) |

### Tests

`php tests/run-tests.php` (after `composer install`) starts the server on ports 18443-18446 and checks pull
distribution, UDP notifications, heartbeats and a failing UDP bind. Exit code 0 means all passed.

Here is an example supervisor config:
 ```
 [program:poker-msg]
command                 = bash -c "ulimit -n 10000; exec /usr/bin/php /home/poker/msg/bin/msg-server.php"
process_name            = poker-msg
numprocs                = 1
autostart               = true
autorestart             = true
user                    = root
stdout_logfile          = /home/poker/logs/poker-msg_info.log
stdout_logfile_maxbytes = 1MB
stderr_logfile          = /home/poker/logs/poker-msg_error.log
stderr_logfile_maxbytes = 1MB
 ```
