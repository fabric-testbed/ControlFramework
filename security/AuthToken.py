#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


from security.Credentials import Credentials


class AuthToken:
    """
    Represents the Authentication Token for a user
    """
    def __init__(self):
        self.name = None
        self.guid = None
        self.cred = None
        self.cert = None
        self.loginToken = None

    def __init__(self, name: str):
        self.name = name
        self.guid = None
        self.cred = None
        self.cert = None
        self.loginToken = None

    def __init__(self, name: str, guid: str):
        self.name = name
        self.guid = guid
        self.cred = None
        self.cert = None
        self.loginToken = None

    def __init__(self, name: str, guid: str, cred: Credentials):
        self.name = name
        self.guid = guid
        self.cred = cred
        self.cert = None
        self.loginToken = None

    def get_name(self):
        """
        Gets the name of the identity represented by this token.

        Returns:
            identity name
        """
        return self.name

    def get_guid(self):
        """
        Gets the guid for the identity represented by this token.

        Returns:
            guid
        """
        return self.guid

    def get_credentials(self):
        """
        Gets the identity credentials

        Returns:
            identity credentials
        """
        return self.cred

    def set_credentials(self, cred: Credentials):
        """
        Sets the identity credentials

        Args:
            cred: identity credentials
        """
        self.cred = cred

    def get_certificate(self):
        """
        Gets the identity certificate

        Returns:
            identity certificate
        """
        return self.cert

    def set_certificate(self, cert: str):
        """
        Sets the identity certificate

        Args:
            cert: identity certificate
        """
        self.cert = cert

    def get_login_token(self):
        """
        Gets the identity token

        Returns:
            identity token
        """
        return self.loginToken

    def set_login_token(self, token: str):
        """
        Sets the identity token

        Args:
            token: identity token
        """
        self.loginToken = token
