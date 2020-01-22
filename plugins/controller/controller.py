#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

import plugins.apis.IActorPlugin as ActorPluginType


class ControllerPlugin(ActorPluginType.IActorPlugin):
    """
    Plugin One

    This is an example of plugin class to depict plugin functionality. It is derived from IActorPlugin base class.
    Any actor specific functionality should be implemented here by overriding the base class functions.

    """
    def __init__(self):
        # Make sure to call the parent class (`IActorPlugin`) methods when
        # overriding them.
        super(ControllerPlugin, self).__init__()

    def print_name(self):
        print("This is ControllerPlugin")
