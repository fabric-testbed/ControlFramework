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
from datetime import timedelta, datetime, timezone
from http.client import BAD_REQUEST

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models import Resource
from fabric_cf.orchestrator.swagger_server.models.resources import Resources  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, RESOURCES_PATH, PORTAL_RESOURCES_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_error_response, cors_success_response


def portalresources_get(graph_format: str, level: int = 1, force_refresh: bool = False, start_date: str = None,
                        end_date: str = None, includes: str = None, excludes: str = None) -> Resources:  # noqa: E501
    """Retrieve a listing and description of available resources for portal

    Retrieve a listing and description of available resources for portal # noqa: E501

    :param graph_format: graph format
    :type graph_format: str
    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool
    :param start_date: starting date to check availability from
    :type start_date: str
    :param end_date: end date to check availability until
    :type end_date: str
    :param includes: comma separated lists of sites to include
    :type includes: str
    :param excludes: comma separated lists of sites to exclude
    :type excludes: str

    :rtype: Resources
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, PORTAL_RESOURCES_PATH).inc()
    try:
        start = handler.validate_lease_time(lease_time=start_date)
        end = handler.validate_lease_time(lease_time=end_date)

        # Check if 'start' is defined but 'end' is not
        if start and not end:
            now = datetime.now(timezone.utc)

            # Check if the current time is within 10 minutes from 'start'
            if now - start < timedelta(minutes=10):
                # Reset start to None so as the cache is used
                start = None

        if start and end and (end - start) < timedelta(minutes=60):
            raise OrchestratorException(http_error_code=BAD_REQUEST,
                                        message="Time range should be at least 60 minutes long!")

        model = handler.list_resources(graph_format_str=graph_format, level=level, force_refresh=force_refresh,
                                       start=start, end=end, includes=includes, excludes=excludes, authorize=False)
        response = Resources()
        response.data = [Resource(model)]
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


def resources_get(level: int = 1, force_refresh: bool = False, start_date: str = None,
                  end_date: str = None, includes: str = None, excludes: str = None) -> Resources:  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501

    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool
    :param start_date: starting date to check availability from
    :type start_date: str
    :param end_date: end date to check availability until
    :type end_date: str
    :param includes: comma separated lists of sites to include
    :type includes: str
    :param excludes: comma separated lists of sites to exclude
    :type excludes: str

    :rtype: Resources
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, RESOURCES_PATH).inc()
    try:
        token = get_token()
        start = handler.validate_lease_time(lease_time=start_date)
        end = handler.validate_lease_time(lease_time=end_date)

        # Check if 'start' is defined but 'end' is not
        if start and not end:
            now = datetime.now(timezone.utc)

            # Check if the current time is within 10 minutes from 'start'
            if now - start < timedelta(minutes=10):
                # Reset start to None so as the cache is used
                start = None

        if start and end and (end - start) < timedelta(minutes=60):
            raise OrchestratorException(http_error_code=BAD_REQUEST,
                                        message="Time range should be at least 60 minutes long!")

        model = handler.list_resources(token=token, level=level, force_refresh=force_refresh,
                                       start=start, end=end, includes=includes, excludes=excludes)
        response = Resources()
        response.data = [Resource(model)]
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
