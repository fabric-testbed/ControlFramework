import connexion

from fabric_cf.orchestrator.swagger_server.models.poa import Poa  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.poa_post import PoaPost  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import poas_controller as rc


def poas_create_sliver_id_post(body, sliver_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request to perform an operation action on a sliver. Supported actions include - reboot a VM sliver, get cpu info, get numa info, pin vCPUs, pin memory to a numa node etc, add/remove ssh keys.    # noqa: E501

    :param body: Perform Operation Action
    :type body: dict | bytes
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Poa
    """
    if connexion.request.is_json:
        body = PoaPost.from_dict(connexion.request.get_json())  # noqa: E501
    return rc.poas_create_sliver_id_post(body=body, sliver_id=sliver_id)


def poas_get(sliver_id=None, states=None, limit=None, offset=None):  # noqa: E501
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
    return rc.poas_get(sliver_id=sliver_id, states=states, limit=limit, offset=offset)


def poas_poa_id_get(poa_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request get the status of the POA identified by poa_id.    # noqa: E501

    :param poa_id: Poa Id for the POA triggered
    :type poa_id: str

    :rtype: Poa
    """
    return rc.poas_poa_id_get(poa_id=poa_id)
