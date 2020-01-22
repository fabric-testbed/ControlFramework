#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis.IProxy import IProxy
from security import AuthToken


class ICallbackProxy(IProxy):
    """
    ICallbackProxy represents the proxy callback interface to an actor.
    """
    def prepare_query_result(self, request_id: str, response, caller: AuthToken):
        """
        Prepare the query result

        Args:
            request_id: request id
            caller: caller
        """
        return None

    def prepare_failed_request(self, request_id: str, failed_request_type, failed_reservation, error: str, caller: AuthToken):
        """
        Prepare the failed response

        Args:
            request_id: request id
            failed_request_type: request type of the failed request
            failed_reservation: failed reservation
            error: error string
            caller: caller
        """
        return None
