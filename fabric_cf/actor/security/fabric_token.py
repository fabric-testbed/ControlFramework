import json
import traceback

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class TokenException(Exception):
    """
    Token exception
    """


class JWTManager:
    """
    This class fetches keys retrieved from a specified endpoint
    and uses them to validate provided JWTs
    """
    @staticmethod
    def decode(*, id_token: str, jwks_url: str) -> dict:
        """
        Decode and validate a JWT
        :raises Exception in case of failure
        """
        try:
            response = requests.get(jwks_url)
            if response.status_code != 200:
                raise TokenException("Failed to get Json Web Keys")

            jwks = response.json()
            public_keys = {}
            for jwk in jwks['keys']:
                kid = jwk['kid']
                public_keys[kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

            kid = jwt.get_unverified_header(id_token)['kid']
            alg = jwt.get_unverified_header(id_token)['alg']
            key = public_keys[kid]

            options = {'verify_aud': False}
            claims = jwt.decode(id_token, key=key, verify=True, algorithms=[alg], options=options)

            return claims
        except Exception as ex:
            raise TokenException(ex)


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, jwks_url: str, token: str, logger):
        if jwks_url is None or token is None:
            raise TokenException('Either jwks_url: {} or token: {} is None'.format(jwks_url, token))

        self.jwks_url = jwks_url
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
            self.decoded_token = JWTManager.decode(id_token=self.encoded_token, jwks_url=self.jwks_url)
            self.logger.debug(json.dumps(self.decoded_token))

            return self.decoded_token
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))
            raise e
