#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.util.ResourceType import ResourceType


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
    def __init__(self):
        return

    def get_config_properties(self):
        """
        Returns the slice configuration properties list

        Returns:
            configuration properties list
        """
        return None

    def get_local_properties(self):
        """
        Returns the slice local properties list

        Returns:
            local properties list
        """
        return None

    def get_description(self):
        """
        Returns the slice description

        Returns:
            slice description
        """
        return None

    def get_name(self):
        """
        Returns the slice name

        Returns:
            slice name
        """
        return None

    def get_owner(self):
        """
        Returns the slice owner

        Returns:
            slice owner
        """
        return None


    def get_properties(self):
        """
        Returns the slice properties

        Returns:
            slice properties
        """
        return None

    def get_request_properties(self):
        """
        Returns the slice request properties list

        Returns:
            slice request properties list
        """
        return None

    def get_resource_properties(self):
        """
        Returns the slice resource properties list

        Returns:
            slice resource properties list
        """
        return None

    def get_resource_type(self):
        """
        Returns the resource type of the slice (if any).

        Returns:
            slice resource type
        """
        return None

    def get_slice_id(self):
        """
        Returns the slice id.

        Returns:
            slice id
        """
        return None

    def is_broker_client(self):
        """
        Checks if the slice is a broker client slice (a client slice within an authority that represents a broker).

        Returns:
            true if the slice is a broker client slice
        """
        return False

    def is_client(self):
        """
        Checks if the slice is a client slice.

        Returns:
            true if the slice is a client slice
        """
        return False

    def is_inventory(self):
        """
        Checks if the slice is a inventory slice.

        Returns:
            true if the slice is a inventory slice
        """
        return False

    def set_broker_client(self):
        """
        Marks the slice as a broker client slice (a client slice within an authority that represents a broker).
        """
        return

    def set_client(self):
        """
        Marks the slice as a client slice.
        """
        return

    def set_inventory(self, value: bool):
        """
        Sets the inventory flag.

        Args:
            value: inventory status: true, inventory slice, false, client slice
        """
        return

    def set_description(self, description: str):
        """
        Sets the slice description.

        Args:
            description: description
        """
        return

    def set_name(self, name: str):
        """
        Sets the slice name.

        Args:
            name: name
        """
        return

    def set_owner(self, owner: str):
        """
        Sets the slice owner.

        Args:
            owner: owner
        """
        return

    def set_slice_id(self, slice_id: str):
        """
        Sets the slice id.

        Args:
            slice_id: slice id
        """
        return

    def set_resource_type(self, resource_type: ResourceType):
        """
        Sets the resource type.

        Args:
            resource_type: resource type
        """
        return
