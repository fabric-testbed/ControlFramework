#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


class ResourceType:
    """
    ResourceType is used to name a particular resource type. ResourceType consists of a
    single string describing the particular resource type. ResourceType is a read-only class:
    once created it cannot be modified.
    """
    def __init__(self):
        self.type = None
        return

    def __init__(self, type: str):
        self.type = type

    def get_type(self):
        return self.type
