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

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models import Resource
from fabric_cf.orchestrator.swagger_server.models.resources import Resources  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, RESOURCES_PATH, PORTAL_RESOURCES_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_error_response, cors_success_response


def portalresources_get(graph_format) -> Resources:  # noqa: E501
    """Retrieve a listing and description of available resources for portal

    Retrieve a listing and description of available resources for portal # noqa: E501

    :param graph_format: graph format
    :type graph_format: str

    :rtype: Resources
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
    try:
        bqm_dict = handler.portal_list_resources(graph_format_str=graph_format)
        response = Resources()
        response.data = [Resource().from_dict(bqm_dict)]
        response.size = 1
        response.type = "resources"
        success_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
        return cors_error_response(error=e)


def resources_get(level, force_refresh) -> Resources:  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501

    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool

    :rtype: Resources
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
    try:
        token = get_token()
        bqm_dict = handler.list_resources(token=token, level=level, force_refresh=force_refresh)
        response = Resources()
        response.data = [Resource().from_dict(bqm_dict)]
        response.size = 1
        response.type = "resources"
        success_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
        return cors_error_response(error=e)
