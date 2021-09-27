from typing import List

from fabric_cf.orchestrator.swagger_server.response import authorization_controller as rc

"""
controller generated to handled auth operation described at:
https://connexion.readthedocs.io/en/latest/security.html
"""
def check_bearerAuth(token):
    return rc.check_bearerAuth(token=token)