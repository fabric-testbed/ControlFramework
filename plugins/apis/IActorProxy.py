#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis.ICallbackProxy import ICallbackProxy
from plugins.apis.IProxy import IProxy
from security import AuthToken


class IActorProxy(IProxy):
    """
    IActorProxy represents the proxy interface to a generic actor
    """
    def prepare_query(self, callback: ICallbackProxy, query, caller: AuthToken):
        """
        Prepares the query

        Args:
            callback: proxy call back which handles the query
            query: query
            caller: caller of the query
        """
        return None
