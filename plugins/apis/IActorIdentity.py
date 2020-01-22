#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


class IActorIdentity:
    """
    IActorIdentity defines the interface required to represent the identity of an actor.
    Each actor is represented by a globally unique identifier and an AuthToken
    """
    def __init__(self):
        return

    def get_guid(self):
        """
        Returns the globally unique identifier for this actor

        Returns:
            actor guid
        """
        return None

    def get_identity(self):
        """
        Returns the identity of the actor

        Returns:
            actor's identity
        """
        return None

    def get_name(self):
        """
        Returns the actor name

        Returns:
            actor name. Note actor names are expected to be unique.
        """
        return None
