from typing import List
import connexion
from fabric.orchestrator.core.orchestrator_handler import OrchestratorHandler

"""
controller generated to handled auth operation described at:
https://connexion.readthedocs.io/en/latest/security.html
"""
def check_bearerAuth(token):
    orchestrator = OrchestratorHandler()
    orchestrator.get_logger().log().debug(connexion.request)
    return orchestrator.validate_credentials(token=token)


