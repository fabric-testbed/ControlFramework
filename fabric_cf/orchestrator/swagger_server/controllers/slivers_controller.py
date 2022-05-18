from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import slivers_controller as rc


def slivers_get(slice_id):  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Slivers
    """
    return rc.slivers_get(slice_id)


def slivers_sliver_id_get(slice_id, sliver_id):  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param sliver_id: Sliver identified by universally unique identifier
    :type sliver_id: str

    :rtype: Slivers
    """
    return rc.slivers_sliver_id_get(slice_id, sliver_id)
