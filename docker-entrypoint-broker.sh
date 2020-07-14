#!/bin/sh
pip install --upgrade pip
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Installing fabric-mesage-bus"
pip3 install git+https://github.com/fabric-testbed/MessageBus.git#egg=fabric-message-bus
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Installing fabric-actor"
mkdir -p /etc/fabric/actor/config
mkdir -p /var/log/actor
pip3 install .
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
cd /usr/src/app/
tail -f /dev/null