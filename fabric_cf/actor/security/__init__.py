# initialize Fabric Logon Token Validation
from datetime import datetime, timedelta

from fss_utils.jwt_validate import JWTValidator

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.globals import GlobalsSingleton
oauth_config = GlobalsSingleton.get().get_config().get_oauth_config()
LOG = GlobalsSingleton.get().get_logger()
CREDMGR_CERTS = oauth_config.get(Constants.property_conf_o_auth_jwks_url, None)
CREDMGR_KEY_REFRESH = oauth_config.get(Constants.property_conf_o_auth_key_refresh, None)
LOG.info(f'Initializing JWT Validator to use {CREDMGR_CERTS} endpoint, '
         f'refreshing keys every {CREDMGR_KEY_REFRESH} HH:MM:SS')
t = datetime.strptime(CREDMGR_KEY_REFRESH, "%H:%M:%S")
jwt_validator = JWTValidator(CREDMGR_CERTS,
                             timedelta(hours=t.hour, minutes=t.minute, seconds=t.second))
