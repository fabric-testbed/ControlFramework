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
from datetime import timedelta, datetime, timezone
from http.client import BAD_REQUEST
from typing import List

from fabric_cf.actor.core.common.constants import Constants

from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
from fabric_cf.orchestrator.swagger_server.models import Status200OkNoContentData, Slice, Sliver, SlicesPost
from fabric_cf.orchestrator.swagger_server.models.slice_details import SliceDetails  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slices import Slices  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_cf.orchestrator.swagger_server import received_counter, success_counter, failure_counter
from fabric_cf.orchestrator.swagger_server.response.constants import POST_METHOD, SLICES_CREATE_PATH, DELETE_METHOD, \
    SLICES_DELETE_PATH, GET_METHOD, SLICES_GET_PATH, SLICES_RENEW_PATH, SLICES_GET_SLICE_ID_PATH, SLICES_MODIFY_PATH, \
    SLICES_MODIFY_ACCEPT_PATH, SLICES_DELETE_SLICE_ID_PATH
from fabric_cf.orchestrator.swagger_server.response.utils import get_token, cors_error_response, cors_success_response


def slices_creates_post(body: SlicesPost, name, lifetime=None, lease_start_time=None, lease_end_time=None):  # noqa: E501
    """Create slice

    Request to create a slice as described in the request, represented as a Graph ML specifying requested resources.
    Resources may be scheduled for immediate or future provisioning. On success, the allocated resources (slivers)
    are returned in Graph ML form and assigned to the requested slice. The orchestrator triggers asynchronous
    provisioning on appropriate sites based on the request timing. Experimenters can invoke the 'get slice' API to
    obtain the latest state of the requested resources.   # noqa: E501

    :param body: Contains the slice details, including requested resources.
    :type body: dict | bytes
    :param name: Slice Name
    :type name: str
    :param lifetime: Optional. The requested slice duration in hours.
    :type lifetime: int
    :param lease_start_time: Requested lease start time for the slice.
    :type lease_start_time: str
    :param lease_end_time: Requested lease end time for the slice.
    :type lease_end_time: str

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()

    try:
        token = get_token()
        ssh_key = ','.join(body.ssh_keys)
        start = handler.validate_lease_time(lease_time=lease_start_time)
        end = handler.validate_lease_time(lease_time=lease_end_time)
        now = datetime.now(timezone.utc)

        # Check that start time is at least 60 minutes in the future
        if start and (start - now) < timedelta(minutes=60):
            raise OrchestratorException(
                http_error_code=BAD_REQUEST,
                message="Requested Start Time should be at least 60 minutes from the current time!"
            )

        # Check for valid lease duration between start and end times
        if start and end:
            diff = end - start
            if diff < timedelta(minutes=60):
                raise OrchestratorException(
                    http_error_code=BAD_REQUEST,
                    message="The requested lease duration must be at least 60 minutes."
                )

            max_duration_hours = Constants.DEFAULT_MAX_DURATION_IN_WEEKS.total_seconds() / 3600  # Convert weeks to hours
            if diff > timedelta(hours=max_duration_hours):
                raise OrchestratorException(
                    http_error_code=BAD_REQUEST,
                    message=f"Requested lease duration exceeds the maximum allowed duration of "
                            f"{max_duration_hours} hours."
                )

        # Set the lifetime to 24 hours if not specified and calculate based on start and end times
        if start and end:
            hours = int((end - start).total_seconds() / 3600)
            if not lifetime:
                lifetime = Constants.DEFAULT_LEASE_IN_HOURS  # Default to 24 hours if unspecified

            # Ensure lifetime does not exceed the specified lease duration
            if lifetime > hours:
                raise OrchestratorException(
                    http_error_code=BAD_REQUEST,
                    message="The specified lifetime exceeds the allowable duration between the start and end times."
                )

        # Create the slice and assemble the response
        slivers_dict = handler.create_slice(token=token, slice_name=name, slice_graph=body.graph_model,
                                            lease_start_time=start, lease_end_time=end,
                                            ssh_key=ssh_key, lifetime=lifetime)
        response = Slivers()
        response.data = [Sliver().from_dict(s) for s in slivers_dict]
        response.size = len(response.data)
        response.type = "slivers"
        success_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        return cors_success_response(response_body=response)

    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        return cors_error_response(error=e)


def slices_delete_delete():  # noqa: E501
    """Delete all slices for a User within a project.

    Delete all slices for a User within a project. User identity email and project id is available in the bearer token.  # noqa: E501


    :rtype: Status200OkNoContent
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
    try:
        token = get_token()
        handler.delete_slices(token=token)
        success_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()

        slice_info = Status200OkNoContentData()
        slice_info.details = f"Slices for user have been successfully deleted"
        response = Status200OkNoContent()
        response.data = [slice_info]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)

    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_PATH).inc()
        return cors_error_response(error=e)


def slices_delete_slice_id_delete(slice_id) -> Status200OkNoContent:  # noqa: E501
    """Delete slice.

    Request to delete slice. On success, resources associated with slice or sliver are stopped if necessary,
    de-provisioned and un-allocated at the respective sites.  # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: Status200OkNoContent
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(DELETE_METHOD, SLICES_DELETE_SLICE_ID_PATH).inc()
    try:
        token = get_token()
        handler.delete_slices(token=token, slice_id=slice_id)
        success_counter.labels(DELETE_METHOD, SLICES_DELETE_SLICE_ID_PATH).inc()

        slice_info = Status200OkNoContentData()
        slice_info.details = f"Slice '{slice_id}' has been successfully deleted"
        response = Status200OkNoContent()
        response.data = [slice_info]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)

    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_SLICE_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(DELETE_METHOD, SLICES_DELETE_SLICE_ID_PATH).inc()
        return cors_error_response(error=e)


def slices_get(name: str = None, search: str = None, exact_match: bool = False,
               as_self: bool = True, states: List[str] = None, limit: int = 5, offset: int = 0):  # noqa: E501
    """Retrieve a listing of user slices

    Retrieve a listing of user slices. It returns list of all slices belonging to all members in a project when
    &#x27;as_self&#x27; is False otherwise returns only the all user&#x27;s slices in a project. # noqa: E501

    :param name: Search for Slices with the name
    :type name: str
    :param search: search term applied
    :type search: str
    :param exact_match: Exact Match for Search term
    :type exact_match: str
    :param as_self: GET object as Self
    :type as_self: bool
    :param states: Search for Slices in the specified states
    :type states: List[str]
    :param limit: maximum number of results to return per page (1 or more)
    :type limit: int
    :param offset: number of items to skip before starting to collect the result set
    :type offset: int

    :rtype: Slices
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
    try:
        token = get_token()
        slices_dict = handler.get_slices(token=token, states=states, name=name, limit=limit, offset=offset,
                                         as_self=as_self, search=search, exact_match=exact_match)
        response = Slices()
        response.data = []
        response.type = 'slices'
        for s in slices_dict:
            slice_obj = Slice().from_dict(s)
            response.data.append(slice_obj)
        response.size = len(response.data)

        success_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_PATH).inc()
        return cors_error_response(error=e)


def slices_modify_slice_id_accept_post(slice_id):  # noqa: E501
    """Accept the last modify an existing slice

    Accept the last modify and prune any failed resources from the Slice.
    Also return the accepted slice model back to the user.   # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_MODIFY_ACCEPT_PATH).inc()
    try:
        token = get_token()
        value = handler.modify_accept(token=token, slice_id=slice_id)
        slice_object = Slice().from_dict(value)
        response = SliceDetails(data=[slice_object], size=1)
        response.type = 'slice_details'
        success_counter.labels(POST_METHOD, SLICES_MODIFY_ACCEPT_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_MODIFY_ACCEPT_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_MODIFY_ACCEPT_PATH).inc()
        return cors_error_response(error=e)


def slices_modify_slice_id_put(body, slice_id):  # noqa: E501
    """Modify an existing slice

    Request to modify an existing slice as described in the request. Request would be a graph ML describing the
    experiment topolgy expected after a modify. The supported modify actions include adding or removing nodes,
    components, network services or interfaces of the slice. On success, one or more slivers are allocated,
    containing resources satisfying the request, and assigned to the given slice. This API returns list and
    description of the resources reserved for the slice in the form of Graph ML. Orchestrator would also trigger
    provisioning of these resources asynchronously on the appropriate sites either now or in the future as requested.
    Experimenter can invoke get slice API to get the latest state of the requested resources.   # noqa: E501

    :param body:
    :type body: dict | bytes
    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str

    :rtype: Slivers
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_MODIFY_PATH).inc()

    try:
        token = get_token()
        slice_graph = body.decode("utf-8")
        slivers_dict = handler.modify_slice(token=token, slice_id=slice_id, slice_graph=slice_graph)
        response = Slivers()
        response.data = []
        for s in slivers_dict:
            sliver = Sliver().from_dict(s)
            response.data.append(sliver)
        response.size = len(response.data)
        response.type = "slivers"
        success_counter.labels(POST_METHOD, SLICES_MODIFY_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_MODIFY_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_CREATE_PATH).inc()
        return cors_error_response(error=e)


def slices_renew_slice_id_post(slice_id, lease_end_time) -> Status200OkNoContent:  # noqa: E501
    """Renew slice

    Request to extend slice be renewed with their expiration extended. If possible, the orchestrator should extend the
    slivers to the requested expiration time, or to a sooner time if policy limits apply.  # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param lease_end_time: New Lease End Time for the Slice
    :type lease_end_time: str

    :rtype: Status200OkNoContent
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()

    try:
        token = get_token()
        end = handler.validate_lease_time(lease_time=lease_end_time)
        handler.renew_slice(token=token, slice_id=slice_id, new_lease_end_time=end)
        success_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()

        slice_info = Status200OkNoContentData()
        slice_info.details = f"Slice '{slice_id}' has been successfully renewed"
        response = Status200OkNoContent()
        response.data = [slice_info]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(POST_METHOD, SLICES_RENEW_PATH).inc()
        return cors_error_response(error=e)


def slices_slice_id_get(slice_id, graph_format, as_self=True) -> SliceDetails:  # noqa: E501
    """slice properties

    Retrieve Slice properties # noqa: E501

    :param slice_id: Slice identified by universally unique identifier
    :type slice_id: str
    :param graph_format: graph format
    :type graph_format: str
    :param as_self: GET object as Self
    :type as_self: bool

    :rtype: SliceDetails
    """
    handler = OrchestratorHandler()
    logger = handler.get_logger()
    received_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
    try:
        token = get_token()
        value = handler.get_slice_graph(token=token, slice_id=slice_id, graph_format_str=graph_format,
                                        as_self=as_self)
        slice_object = Slice().from_dict(value)
        response = SliceDetails(data=[slice_object], size=1)
        response.type = 'slice_details'
        success_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        return cors_success_response(response_body=response)
    except OrchestratorException as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        return cors_error_response(error=e)
    except Exception as e:
        logger.exception(e)
        failure_counter.labels(GET_METHOD, SLICES_GET_SLICE_ID_PATH).inc()
        return cors_error_response(error=e)
