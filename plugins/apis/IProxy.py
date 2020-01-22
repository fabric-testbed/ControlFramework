#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis.IActorIdentity import IActorIdentity


class IProxy(IActorIdentity):
    """
    IProxy defines the base interface each actor proxy must implement.
    """
    def get_type(self):
        """
        Returns the type of proxy

        Returns:
            proxy type
        """
        return None

    def execute(self, request):
        """
        Executes the specified request

        Args:
            request: request

        Raises:
            Exception in case of error
        """
        return
