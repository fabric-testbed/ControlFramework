import json
import traceback

import jwt
from fss_utils.jwt_validate import ValidateCode


class TokenException(Exception):
    """
    Token exception
    """


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, token: str, logger):
        if token is None:
            raise TokenException('Token: {} is None'.format(token))

        self.logger = logger
        self.encoded_token = token
        self.decoded_token = None

    def get_encoded_token(self) -> str:
        """
        Get Encoded token string
        @return encoded token
        """
        return self.encoded_token

    def get_decoded_token(self) -> dict:
        """
        Get Decoded token
        @return Decoded token
        """
        return self.decoded_token

    def validate(self) -> dict:
        """
        Validate the token
        @raise Exception in case of error
        """
        try:
            # validate the token
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            jwt_validator = GlobalsSingleton.get().get_jwt_validator()
            if jwt_validator is not None:
                self.logger.info("Validating CI Logon token")
                code, e = jwt_validator.validate_jwt(self.encoded_token)
                if code is not ValidateCode.VALID:
                    self.logger.error(f"Unable to validate provided token: {code}/{e}")
                    raise e
            else:
                raise TokenException("JWT Token validator not initialized, skipping validation")

            self.decoded_token = jwt.decode(self.encoded_token, verify=False)
            self.logger.debug(json.dumps(self.decoded_token))

            return self.decoded_token
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))
            raise e
