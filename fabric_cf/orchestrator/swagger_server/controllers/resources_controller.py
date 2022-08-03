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


def resources_get(level, force_refresh):  # noqa: E501
    """Retrieve a listing and description of available resources. By default, a cached available resource information is returned. User can force to request the current available resources.

    Retrieve a listing and description of available resources. By default, a cached available resource information is returned. User can force to request the current available resources. # noqa: E501

    :param level: Level of details
    :type level: int
    :param force_refresh: Force to retrieve current available resource information.
    :type force_refresh: bool

    :rtype: Resources
    """
    return rc.resources_get(level, force_refresh)
