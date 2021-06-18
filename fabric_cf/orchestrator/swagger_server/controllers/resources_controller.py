import connexion
import six

from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server import util
from fabric_cf.orchestrator.swagger_server.response import resources_controller as rc


def resources_get(level, graph_format):  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501

    :param level: Level of details
    :type level: int
    :param graph_format: Graph format
    :type graph_format: str

    :rtype: Success
    """
    return rc.resources_get(level, graph_format)
