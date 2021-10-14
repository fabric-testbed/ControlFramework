import unittest

from fim.graph.abc_property_graph import ABCPropertyGraph, GraphFormat
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMFactory, Neo4jCBMGraph
from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin

"""
Test of an ABQM plugin
"""
import yaml
neo4j = None
with open("./config/config.test.yaml", 'r') as stream:
    try:
        config_dict = yaml.safe_load(stream)
        neo4j = config_dict["neo4j"]
    except yaml.YAMLError as exc:
        print(exc)


class ABQM_Test(unittest.TestCase):
    n4j_imp = Neo4jGraphImporter(url=neo4j["url"], user=neo4j["user"],
                                 pswd=neo4j["pass"],
                                 import_host_dir=neo4j["import_host_dir"],
                                 import_dir=neo4j["import_dir"])

    def test_abqm(self):
        self.n4j_imp.delete_all_graphs()
        # these are produced by substrate tests
        site_ads = ['../../../neo4j/Network-ad.graphml', '../../../neo4j/LBNL-ad.graphml',
                    '../../../neo4j/RENCI-ad.graphml',
                    '../../../neo4j/UKY-ad.graphml']

        cbm = Neo4jCBMGraph(importer=self.n4j_imp)

        adm_ids = dict()

        for ad in site_ads:
            plain_neo4j = self.n4j_imp.import_graph_from_file_direct(graph_file=ad)
            print(f"Validating ARM graph {ad}")
            plain_neo4j.validate_graph()

            site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=plain_neo4j.graph_id,
                                                              importer=self.n4j_imp))
            # generate a dict of ADMs from site graph ARM
            site_adms = site_arm.generate_adms()
            print('ADMS' + str(site_adms.keys()))

            # desired ADM is under 'primary'
            site_adm = site_adms['primary']
            cbm.merge_adm(adm=site_adm)

            print('Deleting ADM and ARM graphs')
            for adm in site_adms.values():
                adm_ids[ad] = adm.graph_id
                adm.delete_graph()
            site_arm.delete_graph()

        cbm.validate_graph()
        print('CBM ID is ' + cbm.graph_id)

        cbm_graph_id = cbm.graph_id
        # turn on debug so we can test formation of ABQM without querying
        # actor for reservations
        AggregatedBQMPlugin.DEBUG_FLAG = True

        n4j_pg = Neo4jPropertyGraph(graph_id=cbm_graph_id, importer=self.n4j_imp)

        cbm = Neo4jCBMFactory.create(n4j_pg)

        plugin = AggregatedBQMPlugin(actor=None, logger=None)

        abqm = plugin.plug_produce_bqm(cbm=cbm, query_level=1)

        abqm.validate_graph()

        abqm_string = abqm.serialize_graph()

        print('Writing ABQM to abqm.graphml')
        with open('abqm.graphml', 'w') as f:
            f.write(abqm_string)

        abqm_json = abqm.serialize_graph(format=GraphFormat.JSON_NODELINK)
        print('Writing ABQM to abqm.json')
        with open('abqm.json', 'w') as f:
            f.write(abqm_json)

        print('Writing CBM to cbm.graphml')
        cbm_string = cbm.serialize_graph()
        with open('cbm.graphml', 'w') as f:
            f.write(cbm_string)

        self.n4j_imp.delete_all_graphs()

    def test_cbm(self):
        self.n4j_imp.delete_all_graphs()
        # these are produced by substrate tests
        cbm = '../../../neo4j/abqm-l2-cbm.graphml'

        plain_cbm = self.n4j_imp.import_graph_from_file_direct(graph_file=cbm)
        cbm = Neo4jCBMFactory.create(Neo4jPropertyGraph(graph_id=plain_cbm.graph_id,
                                     importer=self.n4j_imp))
        cbm.validate_graph()

        print('CBM ID is ' + cbm.graph_id)

        cbm_graph_id = cbm.graph_id
        # turn on debug so we can test formation of ABQM without querying
        # actor for reservations
        AggregatedBQMPlugin.DEBUG_FLAG = True

        n4j_pg = Neo4jPropertyGraph(graph_id=cbm_graph_id, importer=self.n4j_imp)

        cbm = Neo4jCBMFactory.create(n4j_pg)

        plugin = AggregatedBQMPlugin(actor=None, logger=None)

        abqm = plugin.plug_produce_bqm(cbm=cbm, query_level=1)

        abqm.validate_graph()

        abqm_string = abqm.serialize_graph()

        print('Writing ABQM to abqm-from-cbm.graphml')
        with open('abqm-from-cbm.graphml', 'w') as f:
            f.write(abqm_string)

        self.n4j_imp.delete_all_graphs()