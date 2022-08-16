from fabric_cf.orchestrator.swagger_server.models.slice_details import SliceDetails  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slices import Slices  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_cf.orchestrator.swagger_server.response import slices_controller as rc


def slices_create_post(body, name, ssh_key, lease_end_time=None):  # noqa: E501
    """Create slice

    Request to create slice as described in the request. Request would be a graph ML describing the requested resources.
    Resources may be requested to be created now or in future. On success, one or more slivers are allocated, containing
    resources satisfying the request, and assigned to the given slice. This API returns list and description of the
    resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger provisioning of these
    resources asynchronously on the appropriate sites either now or in the future as requested. Experimenter can invoke
    get slice API to get the latest state of the requested resources.   # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param name: Slice Name
    :type name: str
    :param ssh_key: User SSH Key
    :type ssh_key: str
    :param lease_end_time: New Lease End Time for the Slice
    :type lease_end_time: str

    :rtype: Slivers
    """
    return rc.slices_create_post(body, name, ssh_key, lease_end_time)


def slices_delete_slice_id_delete(slice_id):  # noqa: E501
    """Delete slice.

    Request to delete slice. On success, resources associated with slice or sliver are stopped if necessary,
    de-provisioned and un-allocated at the respective sites.  # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: Status200OkNoContent
    """
    return rc.slices_delete_slice_id_delete(slice_id)


def slices_get(name=None, states=None, limit=None, offset=None):  # noqa: E501
    """Retrieve a listing of user slices

    Retrieve a listing of user slices # noqa: E501

    :param name: Search for Slices with the name
    :type name: str
    :param states: Search for Slices in the specified states
    :type states: List[str]
    :param limit: maximum number of results to return per page (1 or more)
    :type limit: int
    :param offset: number of items to skip before starting to collect the result set
    :type offset: int

    :rtype: Slices
    """
    return rc.slices_get(name, states, limit, offset)


def slices_modify_slice_id_accept_post(slice_id):  # noqa: E501
    """Accept the last modify an existing slice

    Accept the last modify and prune any failed resources from the Slice. Also return the accepted slice model back to the user.   # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: SliceDetails
    """
    return rc.slices_modify_slice_id_accept_post(slice_id)


def slices_modify_slice_id_put(body, slice_id):  # noqa: E501
    """Modify an existing slice

    Request to modify an existing slice as described in the request. Request would be a graph ML describing the experiment topolgy expected after a modify. The supported modify actions include adding or removing nodes, components, network services or interfaces of the slice. On success, one or more slivers are allocated, containing resources satisfying the request, and assigned to the given slice. This API returns list and description of the resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger provisioning of these resources asynchronously on the appropriate sites either now or in the future as requested. Experimenter can invoke get slice API to get the latest state of the requested resources.   # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: Slivers
    """
    return rc.slices_modify_slice_id_put(body, slice_id)


def slices_renew_slice_id_post(slice_id, lease_end_time):  # noqa: E501
    """Renew slice

    Request to extend slice be renewed with their expiration extended. If possible, the orchestrator should extend the
    slivers to the requested expiration time, or to a sooner time if policy limits apply.  # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param lease_end_time: New Lease End Time for the Slice
    :type lease_end_time: str

    :rtype: Status200OkNoContent
    """
    return rc.slices_renew_slice_id_post(slice_id, lease_end_time)


def slices_slice_id_get(slice_id, graph_format):  # noqa: E501
    """slice properties

    Retrieve Slice properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param graph_format: graph format
    :type graph_format: str

    :rtype: SliceDetails
    """
    return rc.slices_slice_id_get(slice_id, graph_format)
