#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
import connexion
import six

from fabric.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric.orchestrator.swagger_server import util


def slivers_get(slice_id):  # noqa: E501
    """Retrieve a listing of user slivers

    Retrieve a listing of user slivers # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str

    :rtype: Success
    """
    return 'do some magic!'


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
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def slivers_poa_sliver_idpost(sliver_id):  # noqa: E501
    """Perform Operational Action

    Perform the named operational action on the named resources, possibly changing the operational status of the named resources. E.G. &#x27;reboot&#x27; a VM.   # noqa: E501

    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    return 'do some magic!'


def slivers_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers properties

    Retrieve Sliver properties # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    return 'do some magic!'


def slivers_status_sliver_idget(slice_id, sliver_id):  # noqa: E501
    """slivers status

    Retrieve the status of a sliver. Status would include dynamic reservation or instantiation information. This API is used to provide updates on the state of the resources after the completion of create, which began to asynchronously provision the resources. The response would contain relatively dynamic data, not descriptive data as returned in the Graph ML.  # noqa: E501

    :param slice_id: Slice identifier as UUID
    :type slice_id: str
    :param sliver_id: Sliver identifier as UUID
    :type sliver_id: str

    :rtype: Success
    """
    return 'do some magic!'
