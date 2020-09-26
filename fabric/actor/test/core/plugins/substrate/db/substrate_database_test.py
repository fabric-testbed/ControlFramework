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
import logging
import unittest
from datetime import datetime, time

from fabric.actor.core.apis.i_database import IDatabase
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.unit import Unit
from fabric.actor.core.kernel.authority_reservation_factory import AuthorityReservationFactory
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.test.core.plugins.db.actor_database_test import ActorDatabaseTest


class SubstrateDatabaseTest(ActorDatabaseTest):

    Logger = logging.getLogger('AuthorityPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    Logger.setLevel(logging.INFO)

    def make_actor_database(self) -> IDatabase:
        db = SubstrateActorDatabase(user=self.DbUser, password=self.DbPwd, database=self.DbName, db_host=self.DbHost,
                                    logger=self.Logger)
        return db

    def test_d_add_update_get_unit(self):
        actor = self.prepare_actor_database()
        db = actor.get_plugin().get_database()
        slice_obj = SliceFactory.create(slice_id=ID(), name="slice-1")
        db.add_slice(slice_object=slice_obj)

        rset = ResourceSet()
        term = Term(start=datetime.now(), end=datetime.now().replace(minute=20))
        res = AuthorityReservationFactory.create(resources=rset, term=term, slice_obj=slice_obj, rid=ID())
        db.add_reservation(reservation=res)

        rtype = ResourceType(resource_type="12")
        u = Unit(id=ID(), rid=res.get_reservation_id(), slice_id=slice_obj.get_slice_id(), actor_id=actor.get_guid())
        u.set_resource_type(rtype=rtype)
        db.add_unit(u=u)

        self.assertIsNotNone(db.get_unit(unit_id=u.get_id()))
