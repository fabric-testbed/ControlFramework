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
import time
import traceback

import prometheus_client

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.util.graceful_interrupt_handler import GracefulInterruptHandler
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
from fabric_cf.broker.core.broker_kernel import BrokerKernelSingleton

DEBUG_MODE = False


def main():
    """
    Broker entry function
    """
    try:
        # Uncomment when testing as app running
        Globals.config_file = './test.yaml'
        Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'
        Constants.MAINTENANCE_LOCATION = './maintenance.lock'
        with GracefulInterruptHandler() as h:
            GlobalsSingleton.get().start(force_fresh=False)

            runtime_config = GlobalsSingleton.get().get_config().get_runtime_config()
            # prometheus server
            prometheus_port = int(runtime_config.get(Constants.PROPERTY_CONF_PROMETHEUS_REST_PORT, None))
            prometheus_client.start_http_server(prometheus_port)

            actor = GlobalsSingleton.get().get_container().get_actor()
            policy = actor.get_policy()

            if DEBUG_MODE:
                site_ads = ['../../../neo4j/Network-dev.graphml',
                            '../../../neo4j/LBNL.graphml',
                            '../../../neo4j/RENC.graphml',
                            '../../../neo4j/UKY.graphml',
                            '../../../neo4j/AL2S.graphml']

                adm_ids = dict()
                for ad in site_ads:
                    from fabric_cf.actor.fim.fim_helper import FimHelper
                    n4j_imp = FimHelper.get_neo4j_importer()
                    plain_neo4j = n4j_imp.import_graph_from_file_direct(graph_file=ad)
                    print(f"Validating ARM graph {ad}")
                    plain_neo4j.validate_graph()

                    from fim.graph.resources.neo4j_arm import Neo4jARMGraph
                    from fim.graph.neo4j_property_graph import Neo4jPropertyGraph
                    site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=plain_neo4j.graph_id,
                                                                      importer=n4j_imp))
                    # generate a dict of ADMs from site graph ARM
                    site_adms = site_arm.generate_adms()
                    print('ADMS' + str(site_adms.keys()))

                    # desired ADM is under 'primary'
                    site_adm = site_adms['primary']
                    policy.combined_broker_model.merge_adm(adm=site_adm)

                    print('Deleting ADM and ARM graphs')
                    for adm in site_adms.values():
                        adm_ids[ad] = adm.graph_id
                        adm.delete_graph()
                    site_arm.delete_graph()

            while True:
                time.sleep(0.0001)
                BrokerKernelSingleton.get().do_periodic()
                if h.interrupted:
                    GlobalsSingleton.get().stop()
    except Exception:
        traceback.print_exc()


if __name__ == '__main__':
    main()
