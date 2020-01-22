#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################
from plugins.apis.IResponseHandler import IResponseHandler


class IQueryResponseHandler(IResponseHandler):
    """
    IQueryResponseHandler defines the base interface each query response handler must implement.
    """
    def handle(self, status, result):
        return
