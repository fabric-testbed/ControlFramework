#!/bin/sh
#pip install --upgrade pip
#echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
#echo "Installing fabric-message-bus"
#pip3 install git+https://github.com/fabric-testbed/MessageBus.git#egg=fabric-message-bus
#mkdir -p "/etc/fabric/message_bus/schema"
#cp /usr/local/lib/python3.8/site-packages/fabric/message_bus/schema/*.avsc /etc/fabric/message_bus/schema
#echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
#echo "Installing fabric-actor"
#pip3 install git+https://github.com/fabric-testbed/ActorBase.git#egg=fabric-actor
#echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
python3 -m fabric.broker
tail -f /dev/null
