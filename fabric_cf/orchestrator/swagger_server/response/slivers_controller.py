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
from fabric_mb.message_bus.messages.poa_avro import PoaAvro

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.models import Sliver, Poa, PoaData, PoaPost
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, SLIVERS_GET_PATH, \
    SLIVERS_GET_SLIVER_ID_PATH, POST_METHOD, SLIVERS_POA_POST_SLIVER_ID_PATH, SLIVERS_POA_GET_SLIVER_ID_POA_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_error_response, cors_success_response


def slivers_get(slice_id, as_self = True) -> Slivers:  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param as_self: GET object as Self
    :type as_self: bool

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
    try:
        token = get_token()
        slivers_dict = handler.get_slivers(slice_id=slice_id, token=token, as_self=as_self)
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


def slivers_sliver_id_get(slice_id, sliver_id, as_self = True) -> Slivers:  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str
    :param as_self: GET object as Self
    :type as_self: bool

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
    try:
        token = get_token()
        slivers_dict = handler.get_slivers(slice_id=slice_id, token=token, sliver_id=sliver_id, as_self=as_self)
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


def slivers_poa_sliver_id_post(body: PoaPost, sliver_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request to perform an operation action on a sliver. Supported actions include - reboot a VM sliver, get cpu info,
    get numa info, pin vCPUs, pin memory to a numa node etc.    # noqa: E501

    :param body: Perform Operation Action
    :type body: dict | bytes
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Poa
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLIVERS_POA_POST_SLIVER_ID_PATH).inc()
    try:
        token = get_token()
        poa_avro = PoaAvro(operation=body.operation, rid=sliver_id)
        if body.data is not None:
            poa_avro.node_set = body.data.node_set
            poa_avro.vcpu_cpu_map = body.data.vcpu_cpu_map
        poa_id = handler.poa(sliver_id=sliver_id, token=token, poa=poa_avro)
        poa_data = PoaData(request_id=poa_id, operation=body.operation)
        response = Poa()
        response.data = [poa_data]
        response.size = len(response.data)
        response.type = body.operation
        success_counter.labels(POST_METHOD, SLIVERS_POA_POST_SLIVER_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLIVERS_POA_POST_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLIVERS_POA_POST_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)


def slivers_poa_sliver_id_request_id_get(sliver_id, request_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request get the status of the POA identified by request_id.    # noqa: E501

    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str
    :param request_id: Request Id for the POA triggered
    :type request_id: str

    :rtype: Poa
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLIVERS_POA_GET_SLIVER_ID_POA_ID_PATH).inc()
    try:
        token = get_token()
        poa_list = handler.get_poa(sliver_id=sliver_id, token=token, poa_id=request_id)
        response = Poa()
        response.data = []
        for p in poa_list:
            poa = PoaData().from_dict(p)
            response.data.append(poa)
        response.size = len(response.data)
        response.type = "poas"
        success_counter.labels(POST_METHOD, SLIVERS_POA_GET_SLIVER_ID_POA_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLIVERS_POA_GET_SLIVER_ID_POA_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLIVERS_POA_GET_SLIVER_ID_POA_ID_PATH).inc()
        return cors_error_response(error=e)
