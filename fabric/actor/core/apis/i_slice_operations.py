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

from abc import abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.util.id import ID


class ISliceOperations:
    """
    ISliceOperations defines a common set of management operations for slices.
    This interface is implemented by each  actor.
    """
    @abstractmethod
    def get_client_slices(self):
        """
        Returns all client slices registered with the actor.

        Returns:
            an array of client slices
        """

    @abstractmethod
    def get_slices(self):
        """
        Returns all slices registered with the actor.

        Returns:
            an array of slices
        """

    @abstractmethod
    def get_slice(self, *, slice_id: ID) -> ISlice:
        """
        Returns the slice with the given id.

        Args:
            slice_id: slice id
        Returns:
            the slice
        """

    @abstractmethod
    def register_slice(self, *, slice_object: ISlice):
        """
        Registers the slice with the actor. The slice must be a newly created one without a database record.
        If the slice is a recovered/previously unregistered one use re_register_slice(Slice) instead.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def remove_slice(self, *, slice_object: ISlice):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def remove_slice_by_slice_id(self, *, slice_id: ID):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice_id: slice id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def re_register_slice(self, *, slice_object: ISlice):
        """
        Re-registers the slice with the actor. The slice must already have a database record.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def unregister_slice(self, *, slice_object: ISlice):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def unregister_slice_by_slice_id(self, *, slice_id: ID):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close_slice_reservations(self, *, slice_id: ID):
        """
        Close slice reservations

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """
