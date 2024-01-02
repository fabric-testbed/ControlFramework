from typing import Tuple
from fabric_cf.actor.core.common.constants import Constants


class TokenException(Exception):
    """
    Token exception
    """


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, decoded_token: dict, token_hash: str):
        self.decoded_token = decoded_token
        self.hash = token_hash

    @property
    def token_hash(self):
        return self.hash

    @property
    def token(self):
        return self.decoded_token

    @property
    def uuid(self) -> str:
        return self.decoded_token.get(Constants.UUID)

    @property
    def subject(self) -> str:
        """
        Get subject
        @return subject
        """
        return self.decoded_token.get(Constants.CLAIMS_SUB)

    @property
    def email(self) -> str:
        """
        Get email
        @return email
        """
        return self.decoded_token.get(Constants.CLAIMS_EMAIL)

    @property
    def projects(self) -> list or None:
        """
        Get projects
        @return projects
        """
        return self.decoded_token.get(Constants.CLAIMS_PROJECTS)

    def __str__(self):
        return f"Token: {self.decoded_token}"

    @property
    def first_project(self) -> Tuple[str or None, str or None, str or None]:
        if self.projects is None or len(self.projects) == 0:
            return None, None, None

        return self.projects[0].get(Constants.UUID), self.projects[0].get(Constants.TAGS),\
               self.projects[0].get(Constants.NAME)
