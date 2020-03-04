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


import json
import logging

import jwt
import requests
from actor import CONFIG, LOGGER


class AuthToken:
    """
    Represents the Authentication Token for a user
    """
    def __init__(self, id_token):
        self.id_token = id_token
        self.jwks_url = CONFIG.get("oauth", "oauth-jwks-url")
        self.log = logging.getLogger(LOGGER)

    def validate(self):
        try:
            response = requests.get(self.jwks_url)
            if response.status_code !=  200:
                return
            jwks = response.json()
            public_keys = {}
            for jwk in jwks['keys']:
                kid = jwk['kid']
                public_keys[kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

            kid = jwt.get_unverified_header(self.id_token)['kid']
            key = public_keys[kid]
            options = {'verify_aud': False}
            payload = jwt.decode(self.id_token, key=key, algorithms=['RS256'], options=options)
            self.log.debug(json.dumps(payload))
            return payload
        except Exception as e:
            self.log.error(e)
