import connexion

from fabric_cf.orchestrator.swagger_server.models.poa import Poa  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.poa_post import PoaPost  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import slivers_controller as rc


def slivers_get(slice_id, as_self=None):  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param as_self: GET object as Self
    :type as_self: bool

    :rtype: Slivers
    """
    return rc.slivers_get(slice_id=slice_id, as_self=as_self)


def slivers_poa_sliver_id_post(body, sliver_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request to perform an operation action on a sliver. Supported actions include - reboot a VM sliver, get cpu info,
    get numa info, pin vCPUs, pin memory to a numa node etc.    # noqa: E501

    :param body: Perform Operation Action
    :type body: dict | bytes
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Poa
    """
    if connexion.request.is_json:
        body = PoaPost.from_dict(connexion.request.get_json())  # noqa: E501
    return rc.slivers_poa_sliver_id_post(body=body, sliver_id=sliver_id)


def slivers_poa_sliver_id_request_id_get(sliver_id, request_id):  # noqa: E501
    """Perform an operational action on a sliver.

    Request get the status of the POA identified by request_id.    # noqa: E501

    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str
    :param request_id: Request Id for the POA triggered
    :type request_id: str

    :rtype: Poa
    """
    return rc.slivers_poa_sliver_id_request_id_get(sliver_id=sliver_id, request_id=request_id)


def slivers_sliver_id_get(slice_id, sliver_id, as_self=None):  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str
    :param as_self: GET object as Self
    :type as_self: bool

    :rtype: Slivers
    """
    return rc.slivers_sliver_id_get(slice_id=slice_id, sliver_id=sliver_id, as_self=as_self)
