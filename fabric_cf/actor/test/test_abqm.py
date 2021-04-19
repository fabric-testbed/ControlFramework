import unittest

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMFactory
from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin

"""
Test of an ABQM plugin
"""

neo4j = {"url": "bolt://localhost:7687",
         "user": "neo4j",
         "pass": "password",
         "import_host_dir": "/tmp/neo4j1/imports/",
         "import_dir": "/imports"}


class ABQM_Test(unittest.TestCase):
    def test_abqm(self):
        cbm_graph_id = "e599d589-cb1b-4ebd-8caa-b58edd33543f"
        # turn on debug so we can test formation of ABQM without querying
        # actor for reservations
        AggregatedBQMPlugin.DEBUG_FLAG = True

        n4j_imp = Neo4jGraphImporter(url=neo4j["url"], user=neo4j["user"],
                                     pswd=neo4j["pass"],
                                     import_host_dir=neo4j["import_host_dir"],
                                     import_dir=neo4j["import_dir"])

        n4j_pg = Neo4jPropertyGraph(graph_id=cbm_graph_id, importer=n4j_imp)

        cbm = Neo4jCBMFactory.create(n4j_pg)

        plugin = AggregatedBQMPlugin(actor=None, logger=None)

        abqm = plugin.plug_produce_bqm(cbm=cbm, query_level=1)

        abqm.validate_graph()

        abqm_string = abqm.serialize_graph()

        print('Writing ABQM to abqm.graphml')
        with open('abqm.graphml', 'w') as f:
            f.write(abqm_string)