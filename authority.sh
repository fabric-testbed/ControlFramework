#!/usr/bin/env bash

git clone https://github.com/fabric-testbed/InformationModel.git /usr/src/app/InformationModel/
sed -i "/setuptools.setup/a packages=setuptools.find_packages(),include_package_data=True," /usr/src/app/InformationModel/setup.py
pip3 install /usr/src/app/InformationModel/

#pip3 install git+https://github.com/fabric-testbed/InformationModel.git
pip3 install git+https://github.com/fabric-testbed/MessageBus.git@listresources
mkdir -p "/etc/fabric/message_bus/schema"
cp /usr/local/lib/python3.8/site-packages/fabric/message_bus/schema/*.avsc /etc/fabric/message_bus/schema
pip3 install git+https://github.com/fabric-testbed/ControlFramework.git@listresources


python3 -m fabric.authority
tail -f /dev/null
