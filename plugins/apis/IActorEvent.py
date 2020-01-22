#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


class IActorEvent:
    """
     IActorEvent defines the interface for the events handled by an actor.
     """
    def process(self):
        """
        Performs the required action on receipt of a specific event

        Raises:
            Exception in case of failure
        """
        return
