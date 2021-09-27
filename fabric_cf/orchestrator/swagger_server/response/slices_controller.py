from datetime import datetime

import connexion
import six
from http.client import INTERNAL_SERVER_ERROR
from fss_utils.http_errors import cors_response

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response.constants import POST_METHOD, SLICES_CREATE_PATH, \
    SLICES_GET_SLICE_ID_PATH, GET_METHOD, SLICES_GET_PATH, DELETE_METHOD, SLICES_DELETE_PATH, SLICES_RENEW_PATH, \
    SLICE_STATUS_SLICE_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token


def slices_create_post(body, slice_name, ssh_key, lease_end_time):  # noqa: E501
    """Create slice

    Request to create slice as described in the request. Request would be a graph ML describing the requested resources.
    Resources may be requested to be created now or in future. On success, one or more slivers are allocated, containing
    resources satisfying the request, and assigned to the given slice. This API returns list and description of the
    resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger provisioning of these
    resources asynchronously on the appropriate sites either now or in the future as requested. Experimenter can invoke
    get slice API to get the latest state of the requested resources.   # noqa: E501

    :param body:
    :type body: dict | bytes
    :param slice_name: Slice Name
    :type slice_name: str
    :param ssh_key: User SSH Key
    :type ssh_key: str
    :param lease_end_time: Lease End Time for the Slice
    :type lease_end_time: str

    :rtype: Success
    """

    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()

    try:
        token = get_token()
        slice_graph = body.decode("utf-8")
        value = handler.create_slice(token=token, slice_name=slice_name, slice_graph=slice_graph,
                                     ssh_key=ssh_key, lease_end_time=lease_end_time)
        response = Success()
        response.value = value
        success_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slices_delete_slice_iddelete(slice_id):  # noqa: E501
    """Delete slice.

    Request to delete slice. On success, resources associated with slice or sliver are stopped if necessary,
    de-provisioned and un-allocated at the respective sites.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
    try:
        token = get_token()
        handler.delete_slice(token=token, slice_id=slice_id)
        response = Success()
        success_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slices_get(states):  # noqa: E501
    """Retrieve a listing of user slices

    Retrieve a listing of user slices # noqa: E501

    :param states: Slice states
    :type states: List[str]

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slices(token=token, states=states)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slices_modify_slice_idput(body, slice_id):  # noqa: E501
    """Modify slice

    Request to modify slice as described in the request. Request would be a Graph ML describing the requested resources
    for slice or a dictionary for sliver. On success, for one or more slivers are modified. This API returns list and
    description of the resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger
    provisioning of the new resources on the appropriate sites either now or in the future based as requested. Modify
    operations may include add/delete/modify a container/VM/Baremetal server/network or other resources to the slice.
    # noqa: E501

    :param body:
    :type body: dict | bytes
    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    slice_graph = body.decode("utf-8")

    return 'do some magic!'


def slices_redeem_slice_idpost(slice_id):  # noqa: E501
    """Redeem resources reserved via Create API

    Request that the reserved resources be made provisioned, instantiating or otherwise realizing the resources, such
    that they have a valid operational status and may possibly be made ready for experimenter use. This operation is
    synchronous, but may start a longer process, such as creating and imaging a virtual machine.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    return 'do some magic!'


def slices_renew_slice_idpost(slice_id, new_lease_end_time):  # noqa: E501
    """Renew slice

    Request to extend slice be renewed with their expiration extended. If possible, the orchestrator should extend the
    slivers to the requested expiration time, or to a sooner time if policy limits apply.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param new_lease_end_time: New Lease End Time for the Slice
    :type new_lease_end_time: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()

    try:
        token = get_token()
        value = handler.renew_slice(token=token, slice_id=slice_id, new_lease_end_time=new_lease_end_time)
        response = Success()
        response.value = value
        success_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slices_slice_idget(slice_id, graph_format):  # noqa: E501
    """slice properties

    Retrieve Slice properties # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slice_graph(token=token, slice_id=slice_id, graph_format_str=graph_format)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)


def slices_status_slice_idget(slice_id):  # noqa: E501
    """slice status

    Retrieve the status of a slice. Status would include dynamic reservation or instantiation information. This API is
    used to provide updates on the state of the resources after the completion of create, which began to asynchronously
    provision the resources. The response would contain relatively dynamic data, not descriptive data as returned in
    the Graph ML.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLICE_STATUS_SLICE_ID_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slices(token=token, slice_id=slice_id)
        response = Success()
        response.value = value
        success_counter.labels(GET_METHOD, SLICE_STATUS_SLICE_ID_PATH).inc()
        return response
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICE_STATUS_SLICE_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=e.get_http_error_code(), xerror=str(e), body=msg)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICE_STATUS_SLICE_ID_PATH).inc()
        msg = str(e).replace("\n", "")
        return cors_response(status=INTERNAL_SERVER_ERROR, xerror=str(e), body=msg)
