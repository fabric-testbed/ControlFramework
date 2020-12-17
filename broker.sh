#!/usr/bin/env bash

pip3 install git+https://github.com/fabric-testbed/InformationModel.git
pip3 install git+https://github.com/fabric-testbed/MessageBus.git
mkdir -p "/etc/fabric/message_bus/schema"
cp /usr/local/lib/python3.8/site-packages/fabric_mb/message_bus/schema/*.avsc /etc/fabric/message_bus/schema
pip3 install git+https://github.com/fabric-testbed/ControlFramework.git


python3 -m fabric_cf.broker
