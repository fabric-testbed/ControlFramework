# Unit-test tier: fast, self-contained tests that mock all external
# dependencies (HTTP/PDP, database, Kafka, Neo4j) and require NO running
# infrastructure. These are the tests run in CI on every pull request.
#
# Infrastructure-dependent tests live elsewhere under fabric_cf/actor/test and
# should be marked with @pytest.mark.integration so CI can exclude them.
