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
import time
import traceback

import prometheus_client

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
from fabric_cf.actor.core.util.graceful_interrupt_handler import GracefulInterruptHandler


def main():
    """
    Authority entry function
    """
    try:
        # Uncomment when testing as app running
        Globals.config_file = './test-netam.yaml'
        Constants.SUPERBLOCK_LOCATION = './net_state_recovery.lock'
        with GracefulInterruptHandler() as h:
            GlobalsSingleton.get().start(force_fresh=False)

            runtime_config = GlobalsSingleton.get().get_config().get_runtime_config()
            # prometheus server
            prometheus_port = int(runtime_config.get(Constants.PROPERTY_CONF_PROMETHEUS_REST_PORT, None))
            prometheus_client.start_http_server(prometheus_port)

            while True:
                time.sleep(0.0001)
                if h.interrupted:
                    GlobalsSingleton.get().stop()
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':
    main()
