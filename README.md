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
