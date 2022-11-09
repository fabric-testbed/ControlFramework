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
import os
import signal
import time
import traceback

import connexion
import prometheus_client
import waitress
from flask import jsonify

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.util.graceful_interrupt_handler import GracefulInterruptHandler
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.swagger_server import encoder


def main():

    try:
        from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
        with GracefulInterruptHandler() as h:

            GlobalsSingleton.get().start(force_fresh=False)

            while not GlobalsSingleton.get().start_completed:
                time.sleep(0.001)

            from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
            OrchestratorKernelSingleton.get()
            OrchestratorKernelSingleton.get().start_threads()

            runtime_config = GlobalsSingleton.get().get_config().get_runtime_config()

            # prometheus server
            prometheus_port = int(runtime_config.get(Constants.PROPERTY_CONF_PROMETHEUS_REST_PORT, None))
            prometheus_client.start_http_server(prometheus_port)

            rest_port_str = runtime_config.get(Constants.PROPERTY_CONF_CONTROLLER_REST_PORT, None)

            if rest_port_str is None:
                raise OrchestratorException("Invalid configuration rest port not specified")

            print("Starting REST")
            # start swagger
            app = connexion.App(__name__, specification_dir='swagger_server/swagger/')
            app.json = encoder.JSONEncoder
            app.add_api('swagger.yaml', arguments={'title': 'Fabric Orchestrator API'}, pythonic_params=True)

            # Start up the server to expose the metrics.
            waitress.serve(app, port=int(rest_port_str), threads=8)
            while True:
                time.sleep(0.0001)
                if h.interrupted:
                    GlobalsSingleton.get().stop()
                    OrchestratorKernelSingleton.get().stop_threads()
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':
    main()
