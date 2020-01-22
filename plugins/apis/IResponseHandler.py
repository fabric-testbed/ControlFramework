#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


class IResponseHandler:
    """
    IResponseHandler defines the base interface each response handler must implement.
    """
    def __init__(self):
        return