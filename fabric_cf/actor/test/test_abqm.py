import unittest

from fim.graph.abc_property_graph import ABCPropertyGraph, GraphFormat
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMFactory, Neo4jCBMGraph
from fim.user import NodeType

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
        site_ads = ['../../../neo4j/Network-dev.graphml',
                    '../../../neo4j/LBNL.graphml',
                    '../../../neo4j/RENC.graphml',
                    '../../../neo4j/UKY.graphml']

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

        abqm_level2 = plugin.plug_produce_bqm(cbm=cbm, query_level=2)

        abqm_level2.validate_graph()

        abqm_level2_string = abqm_level2.serialize_graph()

        print('Writing ABQM to abqm_level2.graphml')
        with open('abqm_level2.graphml', 'w') as f:
            f.write(abqm_level2_string)

        abqm_level2_json = abqm.serialize_graph(format=GraphFormat.JSON_NODELINK)
        print('Writing ABQM to abqm_level2.json')
        with open('abqm_level2.json', 'w') as f:
            f.write(abqm_level2_json)

        print('Writing CBM to cbm.graphml')
        cbm_string = cbm.serialize_graph()
        with open('cbm.graphml', 'w') as f:
            f.write(cbm_string)

        plain_cbm = self.n4j_imp.import_graph_from_string_direct(graph_string=abqm_level2_string)
        temp = Neo4jCBMFactory.create(Neo4jPropertyGraph(graph_id=plain_cbm.graph_id,
                                                         importer=self.n4j_imp))

        site_node_ids = {}
        for s in temp.get_all_nodes_by_class_and_type(label=ABCPropertyGraph.CLASS_CompositeNode,
                                                      ntype=str(NodeType.Server)):
            labels, props = temp.get_node_properties(node_id=s)
            site_node_ids[props.get('Site')] = s

        ns_node_ids = {}
        for s in temp.get_all_network_service_nodes():
            labels, props = temp.get_node_properties(node_id=s)
            ns_node_ids[props.get('Name')] = s

        path = temp.get_nodes_on_path_with_hops(node_a=site_node_ids['UKY'], node_z=site_node_ids['LBNL'],
                                                hops=[ns_node_ids['RENC_ns']])

        assert(len(path) != 0)
        from fim.user.topology import AdvertizedTopology
        substrate = AdvertizedTopology()
        substrate.load(graph_string=abqm_level2_string)

        uky_node_id = substrate.sites.get("UKY").node_id
        lbnl_node_id = substrate.sites.get("LBNL").node_id
        renc_ns_node_id = substrate.network_services.get("RENC_ns").node_id

        path = substrate.graph_model.get_nodes_on_path_with_hops(node_a=uky_node_id, node_z=lbnl_node_id,
                                                                 hops=[renc_ns_node_id])

        assert(len(path) != 0)

        self.n4j_imp.delete_all_graphs()

    def test_cbm(self):
        self.n4j_imp.delete_all_graphs()
        # these are produced by substrate tests
        #cbm = '../../../neo4j/abqm-l2-cbm.graphml'
        cbm = 'cbm.graphml'

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

        abqm2 = plugin.plug_produce_bqm(cbm=cbm, query_level=2)

        abqm2.validate_graph()

        abqm2_string = abqm2.serialize_graph()

        print('Writing ABQM to abqm2-from-cbm.graphml')
        with open('abqm2-from-cbm.graphml', 'w') as f:
            f.write(abqm2_string)

        self.n4j_imp.delete_all_graphs()