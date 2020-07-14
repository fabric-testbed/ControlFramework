#!/bin/sh
pip install --upgrade pip
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Installing fabric-mesage-bus"
pip3 install git+https://github.com/fabric-testbed/MessageBus.git#egg=fabric-message-bus
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Installing fabric-actor"
pip3 install git+https://github.com/fabric-testbed/ActorBase.git#egg=fabric-actor
echo "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
python3 -m fabric.broker