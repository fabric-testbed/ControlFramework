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
from datetime import datetime, timezone

from fim.user import GraphFormat


class BqmWrapper:
    """
    Implements cache for storing the BQM
    """
    def __init__(self):
        self.graph_format = None
        self.bqm = None
        self.last_query_time = None
        self.refresh_interval_in_seconds = 60

    def set_refresh_interval(self, *, refresh_interval: int):
        """
        Set BQM Refresh Interval
        @param refresh_interval: Refresh Interval in seconds
        """
        self.refresh_interval_in_seconds = refresh_interval

    def can_refresh(self) -> bool:
        """
        Determine if it's time to refresh the BQM
        @return True -> On first attempt or last query time is after refresh Interval; False otherwise
        """
        current_time = datetime.now(timezone.utc)
        if self.last_query_time is None or \
                ((current_time - self.last_query_time).seconds > self.refresh_interval_in_seconds):
            return True
        return False

    def save(self, *, bqm: str, graph_format: GraphFormat):
        """
        Save the BQM
        @param bqm: Broker Query Model
        @param graph_format: Broker Query Model Format
        """
        self.graph_format = graph_format
        self.bqm = bqm
        self.last_query_time = datetime.now(timezone.utc)

    def get_bqm(self) -> str:
        """
        Get BQM
        @return returns BQM
        """
        return self.bqm

    def get_graph_format(self) -> GraphFormat:
        """
        Get BQM Format
        @return bqm format
        """
        return self.graph_format
