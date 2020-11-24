import json
import traceback

import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class TokenException(Exception):
    """
    Token exception
    """


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, token_public_key: str, token: str, logger):
        if token_public_key is None or token is None:
            raise TokenException('Either token_public_key: {} or token: {} is None'.format(token_public_key, token))

        self.token_public_key = token_public_key
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
            with open(self.token_public_key) as f:
                pem_data = f.read()
                f.close()
                key = serialization.load_pem_public_key(data=pem_data.encode("utf-8"),
                                                        backend=default_backend())

            options = {'verify_aud': False}
            verify = True
            self.decoded_token = jwt.decode(self.encoded_token, key=key, algorithms='RS256', options=options,
                                            verify=verify)
            self.logger.debug(json.dumps(self.decoded_token))

            return self.decoded_token
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))
            raise e
