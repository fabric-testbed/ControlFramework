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
import pickle
import threading

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.plugins.db.server_actor_database import ServerActorDatabase
from fabric_cf.actor.core.apis.i_substrate_database import ISubstrateDatabase
from fabric_cf.actor.core.util.id import ID


class SubstrateActorDatabase(ServerActorDatabase, ISubstrateDatabase):
    def get_unit(self, *, unit_id: ID):
        result = None
        try:
            result = self.db.get_unit(act_id=self.actor_id, unt_uid=str(unit_id))
        except Exception as e:
            self.logger.error(e)

        return result

    def add_unit(self, *, u: Unit):
        try:
            if u.get_resource_type() is None:
                raise DatabaseException(Constants.invalid_argument)
            self.lock.acquire()
            if self.get_unit(unit_id=u.get_id()) is not None:
                self.logger.info("unit {} is already present in database".format(u.get_id()))
                return

            slice_id = str(u.get_slice_id())
            parent = self.get_unit(unit_id=u.get_parent_id())
            parent_id = None
            if parent is not None:
                parent_id = parent['unt_id']
            res_id = str(u.get_reservation_id())

            properties = pickle.dumps(u)
            self.db.add_unit(act_id=self.actor_id, slc_guid=slice_id, rsv_resid=res_id,
                             unt_uid=str(u.get_id()), unt_unt_id=parent_id,
                             unt_type=int(str(u.get_resource_type())),
                             unt_state=u.get_state().value, properties=properties)
        finally:
            self.lock.release()

    def get_units(self, *, rid: ID):
        result = None
        try:
            self.lock.acquire()
            result = self.db.get_units(act_id=self.actor_id, rsv_resid=str(rid))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return result

    def remove_unit(self, *, uid: ID):
        try:
            self.lock.acquire()
            self.db.remove_unit(unt_uid=str(uid))
        finally:
            self.lock.release()

    def update_unit(self, *, u: Unit):
        try:
            self.lock.acquire()
            properties = pickle.dumps(u)
            self.db.update_unit(act_id=self.actor_id, unt_uid=str(u.get_id()), properties=properties)
        finally:
            self.lock.release()
