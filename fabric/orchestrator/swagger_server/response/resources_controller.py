import connexion
import six

from fabric.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric.orchestrator.swagger_server import util


def resources_get():  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501


    :rtype: Success
    """
    orchestrator = OrchestratorHandler()
    response = Success()
    response.value = orchestrator.list_resources()
    return response
