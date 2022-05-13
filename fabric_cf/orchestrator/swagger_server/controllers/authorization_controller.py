from typing import List
"""
controller generated to handled auth operation described at:
https://connexion.readthedocs.io/en/latest/security.html
"""
from fabric_cf.orchestrator.swagger_server.response import authorization_controller as rc


def check_bearerAuth(token):
    return rc.check_bearerAuth(token)


