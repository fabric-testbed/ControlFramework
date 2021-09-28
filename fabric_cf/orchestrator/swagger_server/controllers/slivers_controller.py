import connexion
import six

from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server import util
from fabric_cf.orchestrator.swagger_server.response import slivers_controller as rc


def slivers_get(slice_id):  # noqa: E501
    """Retrieve a listing of user slivers
    Retrieve a listing of user slivers # noqa: E501
    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :rtype: Success
    """
    return rc.slivers_get(slice_id)


def slivers_modify_sliver_idput(body, sliver_id, slice_id):  # noqa: E501
    """Modify sliver
    Request to modify slice as described in the request. Request would be a Graph ML describing the requested resources for slice or a dictionary for sliver. On success, for one or more slivers are modified. This API returns list and description of the resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger provisioning of the new resources on the appropriate sites either now or in the future based as requested. Modify operations may include add/delete/modify a container/VM/Baremetal server/network or other resources to the slice.  # noqa: E501
    :param body:
    :type body: dict | bytes
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str
    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :rtype: Success
    """
    return rc.slivers_modify_sliver_idput(body, sliver_id, slice_id)


def slivers_poa_sliver_idpost(body, sliver_id):  # noqa: E501
    """Perform Operational Action
    Perform the named operational action on the named resources, possibly changing the operational status of the named resources. E.G. &#x27;reboot&#x27; a VM.   # noqa: E501
    :param body:
    :type body: dict | bytes
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str
    :rtype: Success
    """
    return rc.slivers_poa_sliver_idpost(body, sliver_id)


def slivers_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers properties
    Retrieve Sliver properties # noqa: E501
    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str
    :rtype: Success
    """
    return rc.slivers_sliver_idget(slice_id, sliver_id)


def slivers_status_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers status
    Retrieve the status of a sliver. Status would include dynamic reservation or instantiation information. This API is used to provide updates on the state of the resources after the completion of create, which began to asynchronously provision the resources. The response would contain relatively dynamic data, not descriptive data as returned in the Graph ML.  # noqa: E501
    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str
    :rtype: Success
    """
    return rc.slivers_status_sliver_idget(slice_id, sliver_id)