from fabric_cf.orchestrator.swagger_server.models.resources import Resources  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import resources_controller as rc


def portalresources_get(graph_format):  # noqa: E501
    """Retrieve a listing and description of available resources for portal

    Retrieve a listing and description of available resources for portal # noqa: E501

    :param graph_format: graph format
    :type graph_format: str

    :rtype: Resources
    """
    return rc.portalresources_get(graph_format)


def resources_get(level):  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501

    :param level: Level of details
    :type level: int

    :rtype: Resources
    """
    return rc.resources_get(level)
