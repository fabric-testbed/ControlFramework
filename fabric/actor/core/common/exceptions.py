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


class DelegationNotFoundException(Exception):
    """
    Delegation Not Found Exception
    """
    def __init__(self, *, text: str = None, did=None):
        super(DelegationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = "Delegation# {} not found".format(did)
        self.did = did

    def __str__(self):
        return self.text


class ReservationNotFoundException(Exception):
    """
    Reservation not found exception
    """
    def __init__(self, *, text: str = None, rid=None):
        super(ReservationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = "Reservation# {} not found".format(rid)
        self.rid = rid

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
            self.text = "Slice# {} not found".format(slice_id)
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
    Authority Exception
    """


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