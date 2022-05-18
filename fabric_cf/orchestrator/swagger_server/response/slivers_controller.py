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
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.models import Sliver
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, SLIVERS_GET_PATH, \
    SLIVERS_GET_SLIVER_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_error_response, cors_success_response


def slivers_get(slice_id) -> Slivers:  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
    try:
        token = get_token()
        slivers_dict = handler.get_slivers(slice_id=slice_id, token=token)
        response = Slivers()
        response.data = []
        for s in slivers_dict:
            sliver = Sliver().from_dict(s)
            response.data.append(sliver)
        response.size = len(response.data)
        response.type = "slivers"
        success_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        return cors_error_response(error=e)


def slivers_sliver_id_get(slice_id, sliver_id) -> Slivers:  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
    try:
        token = get_token()
        slivers_dict = handler.get_slivers(slice_id=slice_id, token=token, sliver_id=sliver_id)
        response = Slivers()
        response.data = []
        for s in slivers_dict:
            sliver = Sliver().from_dict(s)
            response.data.append(sliver)
        response.size = len(response.data)
        response.type = "slivers"
        success_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)
