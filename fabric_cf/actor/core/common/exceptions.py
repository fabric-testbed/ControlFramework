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
from typing import Any


@enum.unique
class ExceptionErrorCode(enum.Enum):
    UNEXPECTED_STATE = enum.auto()
    INVALID_ARGUMENT = enum.auto()
    NOT_FOUND = enum.auto()
    NOT_SUPPORTED = enum.auto()
    NOT_IMPLEMENTED = enum.auto()
    INSUFFICIENT_RESOURCES = enum.auto()
    FAILURE = enum.auto()

    def interpret(self, msg: str = None):
        interpretations = {
            1: "Unexpected state",
            2: "Invalid argument(s)",
            3: "Not found",
            4: "Not supported",
            5: "Not implemented",
            6: "Insufficient resources",
            7: "Failure",
        }
        if msg is None:
            return interpretations[self.value]
        else:
            return interpretations[self.value] + " : " + str(msg)


class DelegationNotFoundException(Exception):
    """
    Delegation Not Found Exception
    """
    def __init__(self, *, text: str = None, did=None):
        super(DelegationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = f"Delegation# {did} not found"
        self.did = did

    def __str__(self):
        return self.text


class ReservationNotFoundException(Exception):
    """
    Reservation not found exception
    """
    def __init__(self, *, text: str = None, rid=None, slice_id=None):
        super(ReservationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            if slice_id is not None:
                self.text = f"Reservation# {rid} not found"
            else:
                self.text = f"Reservation# {rid} not found for slice# {slice_id}"
        self.rid = rid
        self.slice_id = slice_id

    def __str__(self):
        return self.text


class SliceNotFoundException(Exception):
    """
    Slice not found exception
    """
    def __init__(self, *, text: str = None, slice_id=None):
        super(SliceNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = f"Slice# {slice_id} not found"
        self.slice_id = slice_id

    def __str__(self):
        return self.text


class AuthorityException(Exception):
    """
    Authority Exception
    """


class KernelException(Exception):
    """
    Kernel Exception
    """


class ResourcesException(Exception):
    """
    Resources Exception
    """


class BrokerException(Exception):
    """
    Broker Exception
    """

    def __init__(self, *, error_code: ExceptionErrorCode = ExceptionErrorCode.FAILURE, msg: Any = None):
        msg = error_code.interpret(msg=msg)
        super().__init__(msg)
        self.error_code = error_code
        self.msg = msg


class DatabaseException(Exception):
    """
    Database exception
    """


class EventException(Exception):
    """
    Event exception
    """


class ContainerException(Exception):
    """
    Exception raised by container
    """


class InitializationException(Exception):
    """
    Exception raised for Initialization Errors
    """


class KafkaServiceException(Exception):
    """
    Exception raised for Kafka Service Errors
    """


class ActorException(Exception):
    """
    Actor Exception
    """


class ControllerException(Exception):
    """
    Controller Exception
    """


class TicketException(Exception):
    """
    Ticket Exception
    """


class UnitException(Exception):
    """
    Unit Exception
    """


class DelegationException(Exception):
    """
    Delegation Exception
    """


class PluginException(Exception):
    """
    Plugin Exception
    """


class ReservationException(Exception):
    """
    Reservation Exception
    """


class SliceException(Exception):
    """
    Slice Exception
    """


class TickException(Exception):
    """
    Tick Exception
    """


class ManageException(Exception):
    """
    Manage Exception
    """


class PolicyException(Exception):
    """
    Policy Exception
    """


class ProxyException(Exception):
    """
    Proxy Exception
    """


class RegistryException(Exception):
    """
    Registry Exception
    """


class TimeException(Exception):
    """
    Time Exception
    """


class FrameworkException(Exception):
    """
    Framework Exception
    """
