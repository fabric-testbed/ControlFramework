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
from http.client import INTERNAL_SERVER_ERROR

from fss_utils.http_errors import cors_response

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, RESOURCES_PATH, PORTAL_RESOURCES_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token


def portalresources_get(graph_format):  # noqa: E501
    """Retrieve a listing and description of available resources for portal

    Retrieve a listing and description of available resources for portal # noqa: E501

    :param graph_format: Graph format
    :type graph_format: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
    try:
        value = handler.portal_list_resources(graph_format_str=graph_format)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def resources_get(level: int):  # noqa: E501
    """Retrieve a listing and description of available resources

    :param level: Level of details
    :type level: int
    :param graph_format: Graph format
    :type graph_format: str


    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
    try:
        token = get_token()
        value = handler.list_resources(token=token, level=level)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)

