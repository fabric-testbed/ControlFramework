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
from http.client import INTERNAL_SERVER_ERROR, OK

import connexion
import requests
import six
from fss_utils.http_errors import cors_response

from fabric_cf.orchestrator.swagger_server.models.version import Version  # noqa: E501
from fabric_cf.orchestrator.swagger_server import util, received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import VERSIONS_PATH, GET_METHOD


def version_get():  # noqa: E501
    """version

    Version # noqa: E501


    :rtype: Version
    """
    received_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    logger = GlobalsSingleton.get().get_logger()
    try:
        version = '1.0.0'
        tag = '1.0.0'
        url = "https://api.github.com/repos/fabric-testbesd/ControlFramework/git/refs/tags/{}".format(tag)

        response = Version()
        response.version = version
        response.gitsha1 = 'Not Available'

        result = requests.get(url)
        if result.status_code == 200 and result.json() is not None:
            object_json = result.json().get("object", None)
            if object_json is not None:
                sha = object_json.get("sha", None)
                if sha is not None:
                    response.gitsha1 = sha
        success_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=str(e))
    return response
