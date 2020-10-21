import traceback

import connexion
import six

from fabric.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric.orchestrator.swagger_server import util, received_counter, success_counter, failure_counter


def resources_get():  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501


    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels('get', '/resources').inc()
    try:
        value = handler.list_resources()
        response = Success()
        response.value = value
        success_counter.labels('get', '/resources').inc()
        return response
    except Exception as e:
        logger.exception(e)
        failure_counter.labels('get', '/resources').inc()
        return str(e), 500

