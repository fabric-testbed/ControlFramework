from http.client import INTERNAL_SERVER_ERROR

import connexion
import six
from fss_utils.http_errors import cors_response

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import GET_METHOD, SLIVERS_GET_PATH, \
    SLIVERS_GET_SLIVER_ID_PATH, SLIVERS_STATUS_SLIVER_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token


def slivers_get(slice_id):  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slivers(slice_id=slice_id, token=token)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slivers_modify_sliver_idput(body, sliver_id, slice_id):  # noqa: E501
    """Modify sliver

    Request to modify slice as described in the request. Request would be a Graph ML describing the requested resources
    for slice or a dictionary for sliver. On success, for one or more slivers are modified. This API returns list and
    description of the resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger
    provisioning of the new resources on the appropriate sites either now or in the future based as requested.
    Modify operations may include add/delete/modify a container/VM/Baremetal server/network or other resources to
    the slice.  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str
    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    sliver_body = body.decode("utf-8")
    return 'do some magic!'


def slivers_poa_sliver_idpost(body, sliver_id):  # noqa: E501
    """Perform Operational Action

    Perform the named operational action on the named resources, possibly changing the operational status of the named
    resources. E.G. &#x27;reboot&#x27; a VM.   # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    slice_graph = body.decode("utf-8")
    return 'do some magic!'


def slivers_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slivers(slice_id=slice_id, token=token, sliver_id=sliver_id)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_GET_SLIVER_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slivers_status_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers status

    Retrieve the status of a sliver. Status would include dynamic reservation or instantiation information.
    This API is used to provide updates on the state of the resources after the completion of create,
    which began to asynchronously provision the resources. The response would contain relatively dynamic data,
    not descriptive data as returned in the Graph ML.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLIVERS_STATUS_SLIVER_ID_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slivers(slice_id=slice_id, token=token, sliver_id=sliver_id, include_notices=True)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLIVERS_STATUS_SLIVER_ID_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_STATUS_SLIVER_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLIVERS_STATUS_SLIVER_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)
