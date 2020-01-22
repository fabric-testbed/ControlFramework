#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis.ISlice import ISlice


class ISliceOperations:
    """
    ISliceOperations defines a common set of management operations for slices.
    This interface is implemented by each  actor.
    """
    def __init__(self):
        return

    def get_client_slices(self):
        """
        Returns all client slices registered with the actor.

        Returns:
            an array of client slices
        """
        return None

    def get_slices(self):
        """
        Returns all slices registered with the actor.

        Returns:
            an array of slices
        """
        return None

    def get_slice(self, slice_id: str):
        """
        Returns the slice with the given id.

        Args:
            slice_id: slice id
        Returns:
            the slice
        """
        return None

    def register_slice(self, slice:ISlice):
        """
        Registers the slice with the actor. The slice must be a newly created one without a database record.
        If the slice is a recovered/previously unregistered one use re_register_slice(Slice) instead.

        Args:
            slice: slice
        Raises:
            Exception in case of error
        """
        return

    def remove_slice(self, slice: ISlice):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice: slice
        Raises:
            Exception in case of error
        """
        return

    def remove_slice(self, slice_id: str):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice_id: slice id
        Raises:
            Exception in case of error
        """
        return

    def re_register_slice(self, slice: ISlice):
        """
        Re-registers the slice with the actor. The slice must already have a database record.

        Args:
            slice: slice
        Raises:
            Exception in case of error
        """
        return

    def unregister_slice(self, slice: ISlice):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice: slice
        Raises:
            Exception in case of error
        """
        return

    def unregister_slice(self, slice_id: str):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """
        return

    def close_slice_reservations(self, slice_id: str):
        """
        Close slice reservations

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """
        return
