from typing import List

from fabric_cf.actor.security.fabric_token import FabricToken

"""
controller generated to handled auth operation described at:
https://connexion.readthedocs.io/en/latest/security.html
"""
def check_bearerAuth(token):
    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    token = FabricToken(token=token, logger=GlobalsSingleton.get().get_logger())
    return token.validate()



