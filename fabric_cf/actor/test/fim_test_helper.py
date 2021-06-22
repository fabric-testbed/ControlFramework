from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph

import yaml
neo4j = None
with open("./config/config.test.yaml", 'r') as stream:
    try:
        config_dict = yaml.safe_load(stream)
        neo4j = config_dict["neo4j"]
    except yaml.YAMLError as exc:
        print(exc)


class FimTestHelper:
    n4j_imp = Neo4jGraphImporter(url=neo4j["url"], user=neo4j["user"],
                                 pswd=neo4j["pass"],
                                 import_host_dir=neo4j["import_host_dir"],
                                 import_dir=neo4j["import_dir"])

    @staticmethod
    def generate_adms() -> list:
        # these are produced by substrate tests
        site_ads = ['../../../neo4j/RENCI-ad.graphml', '../../../neo4j/UKY-ad.graphml',
                    '../../../neo4j/LBNL-ad.graphml', '../../../neo4j/Network-ad.graphml']

        result = []

        for ad in site_ads:
            plain_neo4j = FimTestHelper.n4j_imp.import_graph_from_file_direct(graph_file=ad)
            print(f"Validating ARM graph {ad}")
            plain_neo4j.validate_graph()

            site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=plain_neo4j.graph_id,
                                                              importer=FimTestHelper.n4j_imp))
            # generate a dict of ADMs from site graph ARM
            site_adms = site_arm.generate_adms()
            print('ADMS' + str(site_adms.keys()))

            # desired ADM is under 'primary'
            site_adm = site_adms['primary']
            result.append(site_adm)
        return result

    @staticmethod
    def generate_renci_adm():
        renci_ad = '../../../neo4j/RENCI-ad.graphml'
        cbm = Neo4jCBMGraph(importer=FimTestHelper.n4j_imp)
        plain_neo4j = FimTestHelper.n4j_imp.import_graph_from_file_direct(graph_file=renci_ad)
        print(f"Validating ARM graph {renci_ad}")
        plain_neo4j.validate_graph()

        site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=plain_neo4j.graph_id,
                                                          importer=FimTestHelper.n4j_imp))
        # generate a dict of ADMs from site graph ARM
        site_adms = site_arm.generate_adms()
        print('ADMS' + str(site_adms.keys()))

        # desired ADM is under 'primary'
        site_adm = site_adms['primary']

        return site_arm, site_adm
