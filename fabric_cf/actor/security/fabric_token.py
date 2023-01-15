import json
import logging
import traceback
from typing import Dict, List, Any, Tuple

from fss_utils.jwt_manager import ValidateCode
from fss_utils.jwt_validate import JWTValidator

from fabric_cf.actor.core.common.constants import Constants


class TokenException(Exception):
    """
    Token exception
    """


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, token: str, jwt_validator: JWTValidator, oauth_config: dict, logger: logging.Logger):
        if token is None:
            raise TokenException('Token: {} is None'.format(token))

        self.logger = logger
        self.jwt_validator = jwt_validator
        self.oauth_config = oauth_config
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
            verify_exp = self.oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_VERIFY_EXP, True)

            if self.jwt_validator is not None:
                self.logger.info("Validating CI Logon token")
                code, token_or_exception = self.jwt_validator.validate_jwt(token=self.encoded_token,
                                                                           verify_exp=verify_exp)
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

    def is_decoded(self) -> bool:
        """
        Check if the token is decoded
        @return True if decoded, False otherwise
        """
        return self.decoded_token is not None

    def get_decoded_token_value(self, key: str) -> Any:
        """
        Get decoded token value
        @param key: key to get value
        @return value
        """
        if self.decoded_token is None:
            self.validate()
        return self.decoded_token.get(key)

    def get_uuid(self) -> str:
        return self.get_decoded_token_value(Constants.UUID)

    def get_subject(self) -> str:
        """
        Get subject
        @return subject
        """
        return self.get_decoded_token_value(Constants.CLAIMS_SUB)

    def get_email(self) -> str:
        """
        Get email
        @return email
        """
        return self.get_decoded_token_value(Constants.CLAIMS_EMAIL)

    def get_projects(self) -> list or None:
        """
        Get projects
        @return projects
        """
        return self.get_decoded_token_value(Constants.CLAIMS_PROJECTS)

    def __str__(self):
        return f"Decoded Token: {self.decoded_token}"

    def get_first_project(self) -> Tuple[str or None, str or None, str or None]:
        projects = self.get_projects()
        if projects is None or len(projects) == 0:
            return None, None, None

        return projects[0].get(Constants.UUID), projects[0].get(Constants.TAGS), projects[0].get(Constants.NAME)
