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
from abc import abstractmethod, ABC
from typing import List

from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.manage.messages.protocol_proxy_mng import ProtocolProxyMng


class ABCComponent(ABC):
    @abstractmethod
    def get_protocols(self) -> List[ProtocolProxyMng]:
        """
        Protocols supported by this components
        @return an list of ProtocolProxyMng
        """

    @abstractmethod
    def get_type_id(self) -> str:
        """
        Type identifier for the component.
        @return returns type id
        """

    @abstractmethod
    def get_last_error(self) -> Error:
        """
        Returns the last error/success for this component.
        @return returns last error/success
        """
