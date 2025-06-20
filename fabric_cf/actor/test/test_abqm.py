import time
import unittest
from datetime import datetime

from fabric_cf.actor.fim.fim_helper import FimHelper

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase


from fim.graph.abc_property_graph import ABCPropertyGraph, GraphFormat
from fim.graph.abc_property_graph_constants import ABCPropertyGraphConstants
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMFactory, Neo4jCBMGraph
from fim.slivers.path_info import Path
from fim.user import NodeType, ERO, ExperimentTopology, Capacities, ComponentModelType, ServiceType

from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin
from fabric_cf.actor.test.base_test_case import BaseTestCase

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


class ABQM_Test(BaseTestCase, unittest.TestCase):
    def get_clean_database(self) -> ActorDatabase:
        db = self.get_actor_database()
        db.set_actor_name(name=self.actor_name)
        db.set_reset_state(state=True)
        db.initialize()
        return db

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

        abqm2 = plugin.plug_produce_bqm(cbm=cbm, query_level=2)

        abqm2.validate_graph()

        abqm2_string = abqm2.serialize_graph()

        print('Writing ABQM to abqm2-from-cbm.graphml')
        with open('abqm2-from-cbm.graphml', 'w') as f:
            f.write(abqm2_string)

        self.n4j_imp.delete_all_graphs()

    def test_load_abqm_level_zero(self):
        self.n4j_imp.delete_all_graphs()
        # these are produced by substrate tests
        cbm = 'cbm-prod.graphml'

        plain_cbm = self.n4j_imp.import_graph_from_file_direct(graph_file=cbm)
        cbm = Neo4jCBMFactory.create(Neo4jPropertyGraph(graph_id=plain_cbm.graph_id,
                                     importer=self.n4j_imp))
        cbm.validate_graph()

        print('CBM ID is ' + cbm.graph_id)

        '''
        cbm_graph_id = cbm.graph_id
        # turn on debug so we can test formation of ABQM without querying
        # actor for reservations
        AggregatedBQMPlugin.DEBUG_FLAG = True

        n4j_pg = Neo4jPropertyGraph(graph_id=cbm_graph_id, importer=self.n4j_imp)

        cbm = Neo4jCBMFactory.create(n4j_pg)

        plugin = AggregatedBQMPlugin(actor=None, logger=None)

        abqm = plugin.plug_produce_bqm(cbm=cbm, query_level=0)

        abqm.validate_graph()

        abqm_string = abqm.serialize_graph()

        cbm.delete_graph()

        plain_abqm = self.n4j_imp.import_graph_from_string_direct(graph_string=abqm_string)
        plain_abqm.validate_graph()

        print('Writing ABQM to abqm-from-cbm.graphml')
        with open('abqm-from-cbm.graphml', 'w') as f:
            f.write(abqm_string)

        #self.n4j_imp.delete_all_graphs()
        '''

    def test_ero_find_paths(self):
        cbm_graph_id = "162bf53f-85c5-498f-bc6f-d7d3109263a8"
        n4j_pg = Neo4jCBMGraph(graph_id=cbm_graph_id, importer=self.n4j_imp)

        hop = "WASH"
        t = ExperimentTopology()
        n1 = t.add_node(name='n1', site='HAWI')
        n2 = t.add_node(name='n2', site='CERN')
        cap = Capacities(core=2, ram=8, disk=100)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n1.add_component(model_type=ComponentModelType.SmartNIC_ConnectX_6, name='n1-nic1')

        n2.add_component(model_type=ComponentModelType.SmartNIC_ConnectX_6, name='n2-nic1')

        ns = t.add_network_service(name='bridge1', nstype=ServiceType.L2PTP, interfaces=[n1.interface_list[0],
                                                                                         n2.interface_list[0]])

        hops = [n1.site, hop, n2.site]
        path = Path()
        path.set_symmetric(hops)
        ero = ERO()
        ero.set(payload=path)
        ns.ero = ero

        type, path = ns.ero.get()
        path_list = path.get()[0]

        source = n4j_pg.get_matching_nodes_with_components(label=ABCPropertyGraphConstants.CLASS_NetworkNode,
                                                           props={ABCPropertyGraphConstants.PROP_SITE: path_list[0]})

        dest = n4j_pg.get_matching_nodes_with_components(label=ABCPropertyGraphConstants.CLASS_NetworkNode,
                                                         props={ABCPropertyGraphConstants.PROP_SITE: path_list[-1]})
        hops = []
        for h in path_list:
            hop = n4j_pg.get_matching_nodes_with_components(label=ABCPropertyGraphConstants.CLASS_NetworkService,
                                                            props={ABCPropertyGraphConstants.PROP_NAME: f"{h}_ns"})
            hops.append(hop[0])

        print(f"HOPS: {hops}")
        final_path = []

        db = self.get_actor_database()


        #shortest_path = n4j_pg.get_nodes_on_shortest_path(node_a=source[0], node_z=dest[0])
        #print(f"Shortest Path: {shortest_path}")

        paths = n4j_pg.get_nodes_on_path_with_hops(node_a=source[0], node_z=dest[0], hops=hops)
        sorted_paths = sorted(paths, key=len)
        print(f"Number of paths: {len(sorted_paths)}")
        for sorted_path in sorted_paths:
            found = True
            links = []
            for item in sorted_path:
                _, props = n4j_pg.get_node_properties(node_id=item)
                #print(props)
                if item.startswith('link:'):
                    links.append(item)
                if props.get("Class") == "NetworkService":
                    final_path.append(f'{props.get("Name")}:{props.get("NodeID")}')

            print(f"Links in path: {links}")
            for l in links:
                link_sliver = n4j_pg.build_deep_link_sliver(node_id=l)
                print(f"""
                Link Info:
                  Node ID       : {link_sliver.node_id}
                  Type          : {link_sliver.get_type()}
                  Allowed Caps  : {link_sliver.capacity_allocations}
                  Total Caps    : {link_sliver.capacities}
                """)

                #if link_sliver.get_type() == LinkType.L1Path:
                #    print(link_sliver)
                #    break
            if found:
                break

        print(f"FINAL - Path: {final_path}")

        '''
        print("=============================================================================================")
        print(f"Number of paths: {len(sorted_paths)}")
        paths = n4j_pg.get_nodes_on_path_with_hops(node_a=dest[0], node_z=source[0], hops=hops)
        sorted_paths = sorted(paths, key=len)
        for sorted_path in sorted_paths:
            links = [item for item in sorted_path if item.startswith('link:')]
            print(f"Links in path: {links}")
            for l in links:
                _, props = n4j_pg.get_node_properties(node_id=l)
                print(props)
                if props['Type'] != 'L2Path':
                    break
            break
        '''