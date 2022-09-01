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
from fabric_cf.orchestrator.swagger_server.models import VersionData
from fabric_cf.orchestrator.swagger_server.models.version import Version  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter, __API_REFERENCE__
from fabric_cf.orchestrator.swagger_server.response.constants import VERSIONS_PATH, GET_METHOD

from fabric_cf import __VERSION__
from fabric_cf.orchestrator.swagger_server.response.cors_response import cors_500, cors_200


def version_get() -> Version:  # noqa: E501
    """version

    Version # noqa: E501


    :rtype: Version
    """
    received_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    logger = GlobalsSingleton.get().get_logger()
    try:
        version = VersionData()
        version.reference = __API_REFERENCE__
        version.version = __VERSION__
        response = Version()
        response.data = [version]
        response.size = len(response.data)
        response.status = 200
        response.type = 'version'
        success_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
        return cors_200(response_body=response)
    except Exception as exc:
        details = 'Oops! something went wrong with version_get(): {0}'.format(exc)
        logger.error(details)
        failure_counter.labels(GET_METHOD, VERSIONS_PATH).inc()
        return cors_500(details=details)
