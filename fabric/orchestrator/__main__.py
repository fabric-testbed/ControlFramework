#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
import logging
import time
import traceback

import connexion

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.util.graceful_interrupt_handler import GracefulInterruptHandler
from fabric.orchestrator.swagger_server import encoder


def main():

    try:
        from fabric.actor.core.container.globals import Globals, GlobalsSingleton
        Globals.ConfigFile = "/Users/komalthareja/renci/code/fabric/ActorBase/fabric/orchestrator/config.orchestrator.yaml"
        with GracefulInterruptHandler() as h:

            GlobalsSingleton.get().start(True)

            from fabric.orchestrator.core.orchestrator_state import ControllerStateSingleton
            #ControllerStateSingleton.get().start_threads()

            rest_port = GlobalsSingleton.get().get_config().get_runtime_config().get(
                Constants.PropertyConfControllerRestPort, None)

            if rest_port is None:
                raise Exception("Invalid configuration rest port not specified")

            print("Starting REST")

            # start swagger
            app = connexion.App(__name__, specification_dir='swagger_server.swagger/')
            app.app.json_encoder = encoder.JSONEncoder
            app.add_api('swagger.yaml', arguments={'title': 'Fabric Orchestrator API'}, pythonic_params=True)
            app.run(port=rest_port)

            while True:
                time.sleep(0.0001)
                if h.interrupted:
                    GlobalsSingleton.get().stop()
                    ControllerStateSingleton.get().stop_threads()
    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':
    main()
