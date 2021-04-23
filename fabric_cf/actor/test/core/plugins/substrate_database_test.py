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
from datetime import datetime

from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.kernel.authority_reservation import AuthorityReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.test.core.plugins.actor_database_test import ActorDatabaseTest


class SubstrateDatabaseTest(ActorDatabaseTest):

    logger = logging.getLogger('AuthorityPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    def make_actor_database(self) -> ABCDatabase:
        db = SubstrateActorDatabase(user=self.db_user, password=self.db_pwd, database=self.db_name, db_host=self.db_host,
                                    logger=self.logger)
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
        u = Unit(rid=res.get_reservation_id(), slice_id=slice_obj.get_slice_id(), actor_id=actor.get_guid())
        u.set_resource_type(rtype=rtype)
        db.add_unit(u=u)

        self.assertIsNotNone(db.get_unit(uid=u.get_id()))
