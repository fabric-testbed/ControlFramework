import json
import traceback

from fss_utils.jwt_manager import ValidateCode

from fabric_cf.actor.core.common.constants import Constants


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
        if self.decoded_token is None:
            self.validate()
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
            verify_exp = GlobalsSingleton.get().get_config().get_oauth_config().get(
                Constants.PROPERTY_CONF_O_AUTH_VERIFY_EXP, True)

            if jwt_validator is not None:
                self.logger.info("Validating CI Logon token")
                code, token_or_exception = jwt_validator.validate_jwt(token=self.encoded_token, verify_exp=verify_exp)
                if code is not ValidateCode.VALID:
                    self.logger.error(f"Unable to validate provided token: {code}/{token_or_exception}")
                    raise TokenException(f"Unable to validate provided token: {code}/{token_or_exception}")
            else:
                raise TokenException("JWT Token validator not initialized, skipping validation")

            self.decoded_token = token_or_exception
            self.logger.debug(json.dumps(self.decoded_token))

            return self.decoded_token
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))
            raise e
