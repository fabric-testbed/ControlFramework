from typing import List

from fabric_mb.message_bus.messages.poa_avro import PoaAvro

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.models import PoaData
from fabric_cf.orchestrator.swagger_server.models.poa import Poa  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.poa_post import PoaPost  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response.constants import POST_METHOD, POAS_POST_SLIVER_ID_PATH, \
    POAS_GET_PATH, POAS_GET_POA_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_success_response, cors_error_response


def poas_create_sliver_id_post(body: PoaPost, sliver_id: str):  # noqa: E501
    """Perform an operational action on a sliver.

    Request to perform an operation action on a sliver. Supported actions include - reboot a VM sliver, get cpu info, get numa info, pin vCPUs, pin memory to a numa node etc.    # noqa: E501

    :param body: Perform Operation Action
    :type body: dict | bytes
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Poa
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, POAS_POST_SLIVER_ID_PATH).inc()
    try:
        logger.info(f"KOMAL -- incoming: {body.data}")
        token = get_token()
        poa_avro = PoaAvro(operation=body.operation, rid=sliver_id)
        if body.data is not None:
            poa_avro.node_set = body.data.node_set
            poa_avro.vcpu_cpu_map = body.data.vcpu_cpu_map
            poa_avro.keys = body.data.keys
            poa_avro.bdf = body.data.bdf
        poa_id, slice_id = handler.poa(sliver_id=sliver_id, token=token, poa=poa_avro)
        poa_data = PoaData(poa_id=poa_id, operation=body.operation,
                           sliver_id=sliver_id, slice_id=slice_id)
        response = Poa()
        response.data = [poa_data]
        response.size = len(response.data)
        response.type = body.operation
        success_counter.labels(POST_METHOD, POAS_POST_SLIVER_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_POST_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_POST_SLIVER_ID_PATH).inc()
        return cors_error_response(error=e)


def poas_get(sliver_id: str =None, states: List[str] = None, limit: int = None, offset: int = None):  # noqa: E501
    """Request get the status of the POAs.

    Request get the status of the POAs    # noqa: E501

    :param sliver_id: Search for POAs for a sliver
    :type sliver_id: str
    :param states: Search for POAs in the specified states
    :type states: List[str]
    :param limit: maximum number of results to return per page (1 or more)
    :type limit: int
    :param offset: number of items to skip before starting to collect the result set
    :type offset: int

    :rtype: Poa
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, POAS_GET_PATH).inc()
    try:
        token = get_token()
        poa_list = handler.get_poas(sliver_id=sliver_id, token=token, limit=limit, offset=offset, states=states)
        response = Poa()
        response.data = []
        for p in poa_list:
            poa = PoaData().from_dict(p)
            response.data.append(poa)
        response.size = len(response.data)
        response.type = "poas"
        success_counter.labels(POST_METHOD, POAS_GET_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_GET_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_GET_PATH).inc()
        return cors_error_response(error=e)


def poas_poa_id_get(poa_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request get the status of the POA identified by poa_id.    # noqa: E501

    :param poa_id: Poa Id for the POA triggered
    :type poa_id: str

    :rtype: Poa
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, POAS_GET_POA_ID_PATH).inc()
    try:
        token = get_token()
        poa_list = handler.get_poas(token=token, poa_id=poa_id)
        response = Poa()
        response.data = []
        for p in poa_list:
            poa = PoaData().from_dict(p)
            response.data.append(poa)
        response.size = len(response.data)
        response.type = "poas"
        success_counter.labels(POST_METHOD, POAS_GET_POA_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_GET_POA_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, POAS_GET_POA_ID_PATH).inc()
        return cors_error_response(error=e)
