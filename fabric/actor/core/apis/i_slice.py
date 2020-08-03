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
from __future__ import annotations
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.security.guard import Guard


class ISlice:
    """
    ISlice describes the programming interface to a slice object. Each slice has a name (not necessarily unique)
    and a globally unique identifier. Slices are used to organize groups of reservations. Each reservation belongs
    to exactly one slice. Slices information is passed to upstream actors as part of ticket and lease requests.
    There are several slice types:
        inventory slices: are used to organized reservations that represent an inventory.
                          For example, allocated resources, or resources to be used to satisfy client requests
        client slices: are used on server actors to group reservations representing client requests
                       (allocated/assigned resources).
        broker client slices: are client slices that represent the requests from a broker that acts as a
                              client of the containing actor

    Each slice contains a number of properties lists, which can be used to store properties applicable to all
    reservations associated with the slice. Properties defined in the slice are automatically inherited by reservations.
    Each reservation can also override a property inherited by the slice, but defining it in its appropriate
    properties list.
    """

    def get_config_properties(self):
        """
        Returns the slice configuration properties list

        Returns:
            configuration properties list
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_local_properties(self):
        """
        Returns the slice local properties list

        Returns:
            local properties list
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_description(self):
        """
        Returns the slice description

        Returns:
            slice description
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_name(self):
        """
        Returns the slice name

        Returns:
            slice name
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_owner(self):
        """
        Returns the slice owner

        Returns:
            slice owner
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_properties(self):
        """
        Returns the slice properties

        Returns:
            slice properties
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_request_properties(self):
        """
        Returns the slice request properties list

        Returns:
            slice request properties list
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_resource_properties(self):
        """
        Returns the slice resource properties list

        Returns:
            slice resource properties list
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_resource_type(self):
        """
        Returns the resource type of the slice (if any).

        Returns:
            slice resource type
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_slice_id(self):
        """
        Returns the slice id.

        Returns:
            slice id
        """
        raise NotImplementedError( "Should have implemented this" )

    def is_broker_client(self):
        """
        Checks if the slice is a broker client slice (a client slice within an authority that represents a broker).

        Returns:
            true if the slice is a broker client slice
        """
        raise NotImplementedError( "Should have implemented this" )

    def is_client(self):
        """
        Checks if the slice is a client slice.

        Returns:
            true if the slice is a client slice
        """
        raise NotImplementedError( "Should have implemented this" )

    def is_inventory(self):
        """
        Checks if the slice is a inventory slice.

        Returns:
            true if the slice is a inventory slice
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_broker_client(self):
        """
        Marks the slice as a broker client slice (a client slice within an authority that represents a broker).
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_client(self):
        """
        Marks the slice as a client slice.
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_inventory(self, value: bool):
        """
        Sets the inventory flag.

        Args:
            value: inventory status: true, inventory slice, false, client slice
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_description(self, description: str):
        """
        Sets the slice description.

        Args:
            description: description
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_name(self, name: str):
        """
        Sets the slice name.

        Args:
            name: name
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_owner(self, owner: AuthToken):
        """
        Sets the slice owner.

        Args:
            owner: owner
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_resource_type(self, resource_type: ResourceType):
        """
        Sets the resource type.

        Args:
            resource_type: resource type
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_guard(self) -> Guard:
        """
        Returns the slice guard.
       
        @return the guard
        """
        raise NotImplementedError("Should have implemented this")

    def set_guard(self, g: Guard):
        """
        Sets the slice guard.
       
        @param g the guard
        """
        raise NotImplementedError("Should have implemented this")

    def clone_request(self):
        """
        Makes a minimal clone of the slice object sufficient for
        cross-actor calls.
       
        @return a slice object to use when making cross-actor calls.
        """
        raise NotImplementedError("Should have implemented this")

    def set_local_properties(self, value: dict):
        raise NotImplementedError("Should have implemented this")

    def set_config_properties(self, value: dict):
        raise NotImplementedError("Should have implemented this")

    def set_request_properties(self, value: dict):
        raise NotImplementedError("Should have implemented this")

    def set_resource_properties(self, value: dict):
        raise NotImplementedError("Should have implemented this")