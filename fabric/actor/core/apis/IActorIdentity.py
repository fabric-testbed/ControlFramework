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
    from fabric.actor.core.util.ID import ID
    from fabric.actor.security.AuthToken import AuthToken


class IActorIdentity:
    """
    IActorIdentity defines the interface required to represent the identity of an actor.
    Each actor is represented by a globally unique identifier and an AuthToken
    """

    def get_guid(self) -> ID:
        """
        Returns the globally unique identifier for this actor

        Returns:
            actor guid
        """
        raise NotImplementedError("Should have implemented this")

    def get_identity(self) -> AuthToken:
        """
        Returns the identity of the actor

        Returns:
            actor's identity
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_name(self) -> str:
        """
        Returns the actor name

        Returns:
            actor name. Note actor names are expected to be unique.
        """
        raise NotImplementedError( "Should have implemented this" )