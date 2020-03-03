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

from swagger_server.models.error import Error  # noqa: E501
from swagger_server.models.success import Success  # noqa: E501
from swagger_server import util


def create_post(body, resource_id):  # noqa: E501
    """creates resource(s)

    Request to create resource(s)  # noqa: E501

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
    """delete resource(s)

    Request to delete resource(s)  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    return 'do some magic!'


def get_get(resource_id):  # noqa: E501
    """get manifest for the resource(s)

    Request to get manifest for resource(s)  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    return 'do some magic!'


def modify_post(body, resource_id):  # noqa: E501
    """modify resource(s)

    Request to modify resource(s)  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: Success
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
