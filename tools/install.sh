#!/usr/bin/sh

echo "0 2 * * * root /usr/local/bin/python3.9 /usr/src/app/cleanup.py -f /etc/fabric/actor/config/config.yaml -d 30 -c slices -o remove" >> /etc/crontab
service cron reload
service cron restart