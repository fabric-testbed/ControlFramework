#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
import datetime

from fabric_cm.credmgr.credmgr_proxy import CredmgrProxy, Status
from fss_utils.jwt_manager import ValidateCode
from fss_utils.jwt_validate import JWTValidator

from fabric_cf.actor.core.util.utils import generate_sha256
from fabric_cf.actor.security.fabric_token import TokenException, FabricToken


class TokenValidator:
    """This class caches revoke list retrieved from a specified endpoint
    and uses it to validate provided tokens"""

    def __init__(self, *, credmgr_host: str, refresh_period: datetime.timedelta,
                 jwt_validator: JWTValidator):
        """ Initialize a validator with an endpoint URL presenting Token revoke list,
        a refresh period for keys expressed as datetime.timedelta and
        audience (i.e. CI Logon client id cilogon:/client_id/1234567890).
        :param credmgr_host:
        :param refresh_period:
        """
        self.credmgr_host = credmgr_host
        assert refresh_period is None or isinstance(refresh_period, datetime.timedelta)
        self.cache_period = refresh_period
        self.trl = None
        self.trl_fetched = None
        self.credmgr_proxy = CredmgrProxy(credmgr_host=credmgr_host)
        self.jwt_validator = jwt_validator

    def __fetch_token_revoke_list(self, *, token):
        """
        Fetch TRL from an endpoint and save it
        @param token token
        """
        if self.trl_fetched is not None:
            if datetime.datetime.now() < self.trl_fetched + self.cache_period:
                return

        status, trl_or_exception = self.credmgr_proxy.token_revoke_list(token=token)
        if status != Status.OK:
            raise Exception(f"Unable to fetch token revoke list: {trl_or_exception}")

        self.trl_fetched = datetime.datetime.now()
        if trl_or_exception is not None:
            self.trl = trl_or_exception
        else:
            self.trl = []

    def validate_token(self, *, token, verify_exp=False) -> FabricToken:
        """
        Validate a token using a JWT Validator
        Returns the decoded token
        :param token:
        :param verify_exp:
        :return decoded token in a dictionary
        """
        if self.jwt_validator is not None:
            self.__fetch_token_revoke_list(token=token)
            code, token_or_exception = self.jwt_validator.validate_jwt(token=token, verify_exp=verify_exp)
            if code is not ValidateCode.VALID:
                raise TokenException(f"Unable to validate provided token: {code}/{token_or_exception}")
        else:
            raise TokenException("JWT Token validator not initialized, skipping validation")

        return FabricToken(decoded_token=token_or_exception, token_hash=generate_sha256(token=token))
