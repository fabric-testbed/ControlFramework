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
from typing import Tuple, Union, List

import requests
from fim.user import GraphFormat

@enum.unique
class Status(enum.Enum):
    OK = 1
    INVALID_ARGUMENTS = 2
    FAILURE = 3


class OrchestratorHelper:
    HTTP_OK = 200
    PROP_DATA = "data"
    PROP_SLIVERS = "slivers"
    PROP_SLICES = "slices"

    def __init__(self):
        self.host = "http://localhost:8700"
        self.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImI0MTUxNjcyMTExOTFlMmUwNWIyMmI1NGIxZDNiNzY2N2U3NjRhNzQ3NzIyMTg1ZTcyMmU1MmUxNDZmZTQzYWEifQ.eyJlbWFpbCI6Imt0aGFyZTEwQGVtYWlsLnVuYy5lZHUiLCJnaXZlbl9uYW1lIjoiS29tYWwiLCJmYW1pbHlfbmFtZSI6IlRoYXJlamEiLCJuYW1lIjoiS29tYWwgVGhhcmVqYSIsImlzcyI6Imh0dHBzOi8vY2lsb2dvbi5vcmciLCJzdWIiOiJodHRwOi8vY2lsb2dvbi5vcmcvc2VydmVyQS91c2Vycy8xMTkwNDEwMSIsImF1ZCI6ImNpbG9nb246L2NsaWVudF9pZC82MTdjZWNkZDc0ZTMyYmU0ZDgxOGNhMTE1MTUzMWRmZiIsImp0aSI6Imh0dHBzOi8vY2lsb2dvbi5vcmcvb2F1dGgyL2lkVG9rZW4vMTQ4NDVkZDI4MTU4YjJlOWQxMjk3YzhmMWRmMzk2ZTcvMTY3MTMwNTA2Nzg5MyIsImF1dGhfdGltZSI6MTY3MTMwNTA2NywiZXhwIjoxNjcxMzA4NzI0LCJpYXQiOjE2NzEzMDUxMjQsInByb2plY3RzIjpbeyJuYW1lIjoiQ0YgVGVzdCIsInV1aWQiOiIxMGMwMDk0YS1hYmFmLTRlZjktYTUzMi0yYmU1M2UyYTg5NmIiLCJ0YWdzIjpbIkNvbXBvbmVudC5HUFUiLCJDb21wb25lbnQuU3RvcmFnZSJdLCJtZW1iZXJzaGlwcyI6eyJpc19jcmVhdG9yIjp0cnVlLCJpc19tZW1iZXIiOnRydWUsImlzX293bmVyIjp0cnVlfX1dLCJyb2xlcyI6W3siZGVzY3JpcHRpb24iOiJUZXN0MiIsIm5hbWUiOiIwZmM3ZGNiOS03MjBiLTRhMDMtYjdlNC01NjFiMDZjNjBkOTgtcGMifSx7ImRlc2NyaXB0aW9uIjoiVGVzdDIiLCJuYW1lIjoiMGZjN2RjYjktNzIwYi00YTAzLWI3ZTQtNTYxYjA2YzYwZDk4LXBvIn0seyJkZXNjcmlwdGlvbiI6IkNGIFRlc3QgYW5kIHRlc3QgYW5kIHRlc3QiLCJuYW1lIjoiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBjIn0seyJkZXNjcmlwdGlvbiI6IkNGIFRlc3QgYW5kIHRlc3QgYW5kIHRlc3QiLCJuYW1lIjoiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBtIn0seyJkZXNjcmlwdGlvbiI6IkNGIFRlc3QgYW5kIHRlc3QgYW5kIHRlc3QiLCJuYW1lIjoiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBvIn0seyJkZXNjcmlwdGlvbiI6IlByb2plY3QtVGFncyIsIm5hbWUiOiJiOGQ2NmZiMy1lN2FkLTRkZjktYTY1Mi0yZDhlMzIzMjM2ZTUtcGMifSx7ImRlc2NyaXB0aW9uIjoiUHJvamVjdC1UYWdzIiwibmFtZSI6ImI4ZDY2ZmIzLWU3YWQtNGRmOS1hNjUyLTJkOGUzMjMyMzZlNS1wbyJ9LHsiZGVzY3JpcHRpb24iOiJBY3RpdmUgVXNlcnMgb2YgRkFCUklDIC0gaW5pdGlhbGx5IHNldCBieSBlbnJvbGxtZW50IHdvcmtmbG93IiwibmFtZSI6ImZhYnJpYy1hY3RpdmUtdXNlcnMifSx7ImRlc2NyaXB0aW9uIjoiRmFjaWxpdHkgT3BlcmF0b3JzIGZvciBGQUJSSUMiLCJuYW1lIjoiZmFjaWxpdHktb3BlcmF0b3JzIn0seyJkZXNjcmlwdGlvbiI6IiBKdXB5dGVyaHViIGFjY2VzcyAtIGJhc2VkIG9uIHByb2plY3QgcGFydGljaXBhdGlvbiIsIm5hbWUiOiJKdXB5dGVyaHViIn0seyJkZXNjcmlwdGlvbiI6IlByb2plY3QgTGVhZHMgZm9yIEZBQlJJQyIsIm5hbWUiOiJwcm9qZWN0LWxlYWRzIn1dLCJzY29wZSI6ImFsbCIsInV1aWQiOiIyMjY1YTRkYS1lMWZjLTRmZmMtYmJiNy1kODZkNTY1NzViZmYifQ.bjSrZgGQDXM4g50wcw-sWEJVMR5g614Od6Sav0Im9fg8MsU68Jpi3uJ8wEkGR8cVO6_FsaYsAhJPDSSuElaeN8bUrkRuCUpx6BoaQ-G7J_JmlAQvH-GIgmYhu9T9epBQLUf_wKFVrbsemEPPzAKofJ58hTchtHTlRWt0hJ7m7eEeBirXJDQeKOSyRFKJ5R61to69tqlf5G_QxuqsaWRcIHdgGxkPlNKNW2aV2aKVvwyde2rbr647NFyXzACpj4ir1aS8WBoTBEhVAa6NV8V9r3QQq6OqKLktj65rRKX4q6DyhEl01QiFBg--JLAAZ2NaOntY4DC8yP_6VnfbvM_8zsYBKdZMgii6Wf0-UNXNEyK47ZfXTCa5v7Tp0ozTNc0_Hd3TFRdNqQ3kCicVZFa671mapBC43lD-pYRloTASUADa89L2VAxxRCl89-i5Gc01le-6ZiUy0moLT_COD8qEQvixnOD4vnb_7VMSiCVSNLzkGM-qCtdMGeYC9PF3y35np1aEkttVGg_mKljsvDp7NMMBcGFP9YJCWVNEQZTvGqvm-HX5iQvXzTXr7jdcxljko1jZH2lHlD9hlNeLazhm6caAiGsSITMXjpzM7rbXfPVf1mEIqIha3NxrQnKDK3vAsWVnFNrOg74LWCwVXZGQggR9yg2XHpV9dBpOscECHHQ"
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
        url = f"{self.host}/resources?level={level}&force_refresh=true"
        return requests.get(url, headers=self.headers, verify=False)

    def portal_resources(self, graph_format: GraphFormat = GraphFormat.JSON_NODELINK):
        url = f"{self.host}/portalresources?graph_format={graph_format.name}"
        headers = {'accept': 'application/json'}
        return requests.get(url, headers=headers, verify=False)

    def create(self, slice_graph: str, slice_name: str,
               lease_end_time: str = None) -> Tuple[Status, Union[list, requests.Response]]:
        if lease_end_time is None:
            url = f"{self.host}/slices/create?name={slice_name}&ssh_key={self.ssh_key}"
        else:
            url = f"{self.host}/slices/create?name={slice_name}&ssh_key={self.ssh_key}&lease_end_time={lease_end_time}"
        response = requests.post(url, headers=self.headers_with_content, verify=False, data=slice_graph)
        if response.status_code == self.HTTP_OK:
            reservations = response.json()[self.PROP_DATA]
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

    def slices(self, slice_id: str = None,
               state: str = "Active") -> Tuple[Status, Union[list, str, requests.Response]]:
        url = f"{self.host}/slices?state={state}"
        if slice_id is not None:
            url = f"{self.host}/slices/{slice_id}?graph_format=GRAPHML"

        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            slices = response.json()[self.PROP_DATA]
            return Status.OK, slices
        else:
            return Status.FAILURE, response

    def slivers(self, slice_id: str, sliver_id: str = None) -> Tuple[Status, Union[list, str, requests.Response]]:
        url = f"{self.host}/slivers?slice_id={slice_id}"
        if sliver_id is not None:
            url = f"{self.host}/slivers/{sliver_id}?slice_id={slice_id}"

        response = requests.get(url, headers=self.headers, verify=False)
        if response.status_code == self.HTTP_OK:
            reservations = response.json()[self.PROP_DATA]
            return Status.OK, reservations
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
            url = f"{self.host}/slices/renew/{slice_id}?new_lease_end_time={new_lease_end_time}"
            response = requests.post(url, headers=self.headers, verify=False)
            if response.status_code != self.HTTP_OK:
                return Status.FAILURE, response

            return Status.OK, response
        except Exception as e:
            return Status.FAILURE, response