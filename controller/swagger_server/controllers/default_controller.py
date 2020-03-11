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
import uuid

import connexion
import six
from yapsy.PluginManager import PluginManagerSingleton

from controller.swagger_server.models.success import Success  # noqa: E501
from controller.swagger_server import util


def create_post(body, resource_id):  # noqa: E501
    """Create slice

    Request to create slice as described in the request. Request would be a graph ML or graph JSON describing the requested resources. Resources may be requested to be created now or in future. On success, one or more slivers are allocated, containing resources satisfying the request, and assigned to the given slice. This API returns list and description of the resources reserved for the slice in the form of manifest. Controller would also trigger provisioning of these resources asynchronously on the appropriate sites either now or in the future as requested. Experimenter can invoke getManifest API to get the latest state of the requested resources.   # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    # Invoke Controller Actor Plugin
    manager = PluginManagerSingleton.get()
    controller_plugin = manager.getPluginByName("controller")
    slice_guid = uuid.uuid1()
    message = controller_plugin.plugin_object.create_slice(slice_guid, resource_id, body)
    ret_val = Success(200, message)
    return ret_val


def delete_delete(resource_id):  # noqa: E501
    """Delete slice or sliver.

    Request to delete sliver or slice. On success, resources associated with slice or sliver are stopped if necessary, de-provisioned and un-allocated at the respective sites.  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    return 'do some magic!'


def get_manifest_get(resource_id):  # noqa: E501
    """Retrieve manifest for the slice or sliver

    Retrieve a manifest describing the resources contained by the name entities, e.g. a single slice or a set of the slivers in a slice. This listing and description should be sufficiently descriptive to allow experimenters to use the resources.  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    return 'do some magic!'


def get_version_get():  # noqa: E501
    """Retrieve the current Controller version and list of versions of the API supported. 

    Retrieve the current Controller version and list of versions of the API supported.   # noqa: E501


    :rtype: Success
    """
    return 'do some magic!'


def list_resources_get():  # noqa: E501
    """Retrieve a listing and description of available resources at various sites. 

    Retrieve a listing and description of available resources at various sites. The resource listing and description provides sufficient information for expermineter/clients to select among available resources.   # noqa: E501


    :rtype: Success
    """
    return 'do some magic!'


def modify_post(body, resource_id):  # noqa: E501
    """Modify slice or sliver

    Request to modify sliver or slice as described in the request. Request would be a graph ML or graph JSON describing the requested resources for slice or a dictionary for sliver. On success, for one or more slivers are modified. This API returns list and description of the resources reserved for the slice in the form of manifest. Controller would also trigger provisioning of the new resources on the appropriate sites either now or in the future based as requested. Modify operations may include add/delete/modify a container/VM/Baremetal server/network or other resources to the sliver or slice.  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def query_resources_post(body):  # noqa: E501
    """Query resource(s)

    Request to query the availability of resources as described in the request. Request would be a graph ML or graph JSON describing the requested resources. On success, controller will return the availability of each requested resource.  # noqa: E501

    :param body: 
    :type body: dict | bytes

    :rtype: Success
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def renew_post(resource_id, new_lease_end_time):  # noqa: E501
    """Renew slice or sliver

    Request to extend sliver or slice be renewed with their expiration extended. If possible, the controller should extend the slivers to the requested expiration time, or to a sooner time if policy limits apply.  # noqa: E501

    :param resource_id: 
    :type resource_id: str
    :param new_lease_end_time: 
    :type new_lease_end_time: str

    :rtype: Success
    """
    return 'do some magic!'


def status_get(resource_id):  # noqa: E501
    """Retrieve status of a slice or sliver(s)

    Retrieve the status of a slice or sliver(s). Status would include dynamic reservation or instantiation information. This API is used to provide updates on the state of the resources after the completion of create, which began to asynchronously provision the resources. The response would contain relatively dynamic data, not descriptive data as returned in the manifest.  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    return 'do some magic!'
