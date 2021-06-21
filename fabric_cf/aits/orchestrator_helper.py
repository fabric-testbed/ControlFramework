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
import enum
from datetime import datetime
from typing import Tuple, Union, List

import requests
from fim.user import GraphFormat

from fabric_cf.actor.core.common.constants import Constants as CFConstants
from fabric_cf.aits.elements.constants import Constants
from fabric_cf.aits.elements.reservation import ReservationFactory, Reservation
from fabric_cf.aits.elements.slice import SliceFactory, Slice


@enum.unique
class Status(enum.Enum):
    OK = 1
    INVALID_ARGUMENTS = 2
    FAILURE = 3


class OrchestratorHelper:
    HTTP_OK = 200

    def __init__(self):
        self.host = "http://localhost:8700"
        self.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImI0MTUxNjcyMTExOTFlMmUwNWIyMmI1NGIxZDNiNzY2N2U3NjRhNzQ3NzIyMTg1ZTcyMmU1MmUxNDZmZTQzYWEifQ.eyJlbWFpbCI6Imt0aGFyZTEwQGVtYWlsLnVuYy5lZHUiLCJnaXZlbl9uYW1lIjoiS29tYWwiLCJmYW1pbHlfbmFtZSI6IlRoYXJlamEiLCJuYW1lIjoiS29tYWwgVGhhcmVqYSIsImlzcyI6Imh0dHBzOi8vY2lsb2dvbi5vcmciLCJzdWIiOiJodHRwOi8vY2lsb2dvbi5vcmcvc2VydmVyQS91c2Vycy8xMTkwNDEwMSIsImF1ZCI6ImNpbG9nb246L2NsaWVudF9pZC8xMjUzZGVmYzYwYTMyM2ZjYWEzYjQ0OTMyNjQ3NjA5OSIsInRva2VuX2lkIjoiaHR0cHM6Ly9jaWxvZ29uLm9yZy9vYXV0aDIvaWRUb2tlbi82ZmMxYTYyNjY5ZmE0NTk4OTExMjY1ODI0OTgxZThkOC8xNjA2NjU4NjE3NzA4IiwiYXV0aF90aW1lIjoiMTYwNjY1ODYxNyIsImV4cCI6MTYxMDgzMDQzNCwiaWF0IjoxNjEwNzQ0MDM0LCJyb2xlcyI6WyJwcm9qZWN0LWxlYWRzIl0sInByb2plY3RzIjp7IlJFTkNJLVRFU1QiOlsidGFnIDEiLCJ0YWcgMiJdfSwic2NvcGUiOiJhbGwifQ.Gttyz2HQohcj0_fOF00RIGUAMiGtdCBfh5IPs3L2KMKL9Rddy7G_zoqtIynpGr58E8Fn4n4ssGOn9Mutas3kJBCZhF_DawI9uNYGxjhVeinE-Rq7r1Mciwcvj8dA4GPASeurU0yWucioNCxx6u-X4IxbGf2Z01ONPfDN09gSKFk9D_oWy-GTEdiwddr2c_AhtxhCJLS1ZQZ2-mY8R6BJMIZsHU_nv8PJb3luPuRD9b8oVN0W1bIm6JorTja7Tz3P5YcD86ZnBJgPLQW65HoXOfKfxmV4gEHx5c-APjQs1LnbpiL4SvOFdOobgTZYDOV9ei03iXlLdw0GymvQmg53GQ"
        self.headers = {
            'accept': 'application/json',
            'Authorization': f"Bearer {self.token}"
        }
        self.headers_with_content = {
            'accept': 'application/json',
            'Authorization': f"Bearer {self.token}",
            'Content-Type': "text/plain"
        }
        self.ssh_key = "empty ssh string"

    def resources(self, level: int = 1):
        url = f"{self.host}/resources?level={level}"
        return requests.get(url, headers=self.headers, verify=False)

    def portal_resources(self, graph_format: GraphFormat = GraphFormat.JSON_NODELINK):
        url = f"{self.host}/portalresources?graphFormat={graph_format.name}"
        headers = {'accept': 'application/json'}
        return requests.get(url, headers=headers, verify=False)

    def create(self, slice_graph: str, slice_name: str,
               lease_end_time: str = None) -> Tuple[Status, Union[List[Reservation], requests.Response]]:
        if lease_end_time is None:
            url = f"{self.host}/slices/create?sliceName={slice_name}&sshKey={self.ssh_key}"
        else:
            url = f"{self.host}/slices/create?sliceName={slice_name}&sshKey={self.ssh_key}&leaseEndTime={lease_end_time}"
        response = requests.post(url, headers=self.headers_with_content, verify=False, data=slice_graph)
        if response.status_code == self.HTTP_OK:
            reservations = ReservationFactory.create_reservations(reservation_list=
                                                                  response.json()[Constants.PROP_VALUE][Constants.PROP_RESERVATIONS])
            return Status.OK, reservations
        else:
            return Status.FAILURE, response

    def delete(self, slice_id: str) -> Tuple[Status, requests.Response]:
        url = f"{self.host}/slices/delete/{slice_id}"
        response = requests.delete(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            return Status.OK, response
        else:
            return Status.FAILURE, response

    def slice_status(self, slice_id: str) -> Tuple[Status, Union[Slice, requests.Response]]:
        url = f"{self.host}/slices/status/{slice_id}"
        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            slices = SliceFactory.create_slices(slice_list=response.json()[Constants.PROP_VALUE][Constants.PROP_SLICES])
            result = None
            if slices is not None and len(slices) > 0:
                result = next(iter(slices))
            return Status.OK, result
        else:
            return Status.FAILURE, response

    def slices(self, slice_id: str = None,
               state: str = "Active") -> Tuple[Status, Union[List[Slice], str, requests.Response]]:
        url = f"{self.host}/slices?state={state}"
        if slice_id is not None:
            url = f"{self.host}/slices/{slice_id}"

        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            if slice_id is None:
                slices = SliceFactory.create_slices(slice_list=response.json()[Constants.PROP_VALUE][Constants.PROP_SLICES])
                return Status.OK, slices
            else:
                slice_graph = response.json()[Constants.PROP_VALUE][Constants.PROP_SLICE_MODEL]
                return Status.OK, slice_graph
        else:
            return Status.FAILURE, response

    def slivers(self, slice_id: str, sliver_id: str = None) -> Tuple[Status, Union[List[Reservation], str, requests.Response]]:
        url = f"{self.host}/slivers?sliceID={slice_id}"
        if sliver_id is not None:
            url = f"{self.host}/slivers/{sliver_id}?sliceID={slice_id}"

        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            reservations = ReservationFactory.create_reservations(reservation_list=
                                                                  response.json()[Constants.PROP_VALUE][Constants.PROP_RESERVATIONS])
            return Status.OK, reservations
        else:
            return Status.FAILURE, response

    def sliver_status(self, slice_id: str, sliver_id: str) -> Tuple[Status, Union[Reservation, requests.Response]]:
        url = f"{self.host}/slivers/status/{sliver_id}?sliceID={slice_id}"
        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            reservations = ReservationFactory.create_reservations(reservation_list=
                                                                  response.json()[Constants.PROP_VALUE][Constants.PROP_RESERVATIONS])
            result = None
            if reservations is not None and len(reservations) > 0:
                result = next(iter(reservations))
            return Status.OK, result
        else:
            return Status.FAILURE, response

    def renew(self, slice_id: str, new_lease_end_time: str) -> Tuple[Status, Union[requests.Response, None]]:
        """
        Renew a slice
        @param token fabric token
        @param slice_id slice_id
        @param new_lease_end_time new_lease_end_time
        @return Tuple containing Status and List of Reservation Id failed to extend
        """
        response = None
        try:
            # Set the tokens
            url = f"{self.host}/slices/renew/{slice_id}?newLeaseEndTime={new_lease_end_time}"
            response = requests.post(url, headers=self.headers, verify=False)
            if response.status_code != self.HTTP_OK:
                return Status.FAILURE, response

            return Status.OK, response
        except Exception as e:
            return Status.FAILURE, response