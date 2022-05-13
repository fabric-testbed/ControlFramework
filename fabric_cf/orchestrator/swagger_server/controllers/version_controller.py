from fabric_cf.orchestrator.swagger_server.models.version import Version  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import version_controller as rc


def version_get():  # noqa: E501
    """Version

    Version # noqa: E501


    :rtype: Version
    """
    return rc.version_get()
