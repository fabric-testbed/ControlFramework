from typing import List

from fabric.orchestrator.core.orchestrator_handler import OrchestratorHandler

"""
controller generated to handled auth operation described at:
https://connexion.readthedocs.io/en/latest/security.html
"""
def check_bearerAuth(token):
    orchestrator = OrchestratorHandler()
    return orchestrator.validate_credentials(token)


