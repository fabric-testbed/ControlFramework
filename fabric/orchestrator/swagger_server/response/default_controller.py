import connexion
import six

from fabric.orchestrator.swagger_server.models.version import Version  # noqa: E501
from fabric.orchestrator.swagger_server import util


def version_get():  # noqa: E501
    """version

    Version # noqa: E501


    :rtype: Version
    """
    response = Version()
    response.version = '1.0.0'
    response.gitsha1 = 'gitsha1'
    return response
