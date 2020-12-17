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
import traceback

import connexion
import six

from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server import util, received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import GetMethod, ResourcesPath
from fabric_cf.orchestrator.swagger_server.response.utils import get_token


def resources_get():  # noqa: E501
    """Retrieve a listing and description of available resources

    Retrieve a listing and description of available resources # noqa: E501


    :rtype: Success
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GetMethod, ResourcesPath).inc()
    try:
        token = get_token()
        value = handler.list_resources(token=token)
        response = Success()
        response.value = value
        success_counter.labels(GetMethod, ResourcesPath).inc()
        return response
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GetMethod, ResourcesPath).inc()
        return str(e), 500

