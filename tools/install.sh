#!/usr/bin/sh
pip install fabric_reports_client
echo "0 * * * * root /usr/local/bin/python3.11 /usr/src/app/export.py --config_file /etc/fabric/actor/config/config.yaml" >> /etc/crontab
echo "0 * * * * root /usr/local/bin/python3.11 /usr/src/app/audit.py -f /etc/fabric/actor/config/config.yaml -a  /etc/fabric/actor/config/vm_handler_config.yml -d 30 -c audit -o audit" >> /etc/crontab
#echo "0 2 * * * root /usr/local/bin/python3.11 /usr/src/app/audit.py -f /etc/fabric/actor/config/config.yaml -d 30 -c slices -o remove" >> /etc/crontab
#echo "*/15 * * * * root /usr/local/bin/python3.11 /usr/src/app/audit.py -f /etc/fabric/actor/config/config.yaml -c slivers -o close" >> /etc/crontab
service cron reload
service cron restart