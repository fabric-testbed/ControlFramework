#!/usr/bin/sh

echo "0 2 * * * root /usr/local/bin/python3.9 /usr/src/app/audit.py -f /etc/fabric/actor/config/config.yaml -d 30 -c slices -o remove" >> /etc/crontab
echo "*/15 * * * * root /usr/local/bin/python3.9 /usr/src/app/audit.py -f /etc/fabric/actor/config/config.yaml -c slivers -o close" >> /etc/crontab
service cron reload
service cron restart