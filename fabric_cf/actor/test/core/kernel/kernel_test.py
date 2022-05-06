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
import unittest
import time

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.kernel_wrapper import KernelWrapper
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.kernel.slice import SliceFactory, SliceTypes
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.test.base_test_case import BaseTestCase


class KernelTest(BaseTestCase, unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.001)

    base_slices_count = 8
    base_client_slices_count = 4
    base_inventory_slices_count = 4
    base_res_count = 8

    def enforceReservationExistsInDatabase(self, *, db: ABCDatabase, rid: ID):
        res = db.get_reservations(rid=rid)
        self.assertIsNotNone(res)
        self.assertEqual(len(res), 1)

    def enforceReservationNotInDatabase(self, *, db: ABCDatabase, rid: ID):
        res = db.get_reservations(rid=rid)
        self.assertIsNone(res)
        self.assertEqual(len(res), 0)

    def enforceSliceExistsInDatabase(self, *, db: ABCDatabase, slice_id: ID):
        slice_obj = db.get_slices(slice_id=slice_id)
        self.assertIsNotNone(slice_obj)
        self.assertEqual(len(slice_obj), 1)

    def enforceSliceNotInDatabase(self, *, db: ABCDatabase, slice_id: ID):
        slice_obj = db.get_slices(slice_id=slice_id)
        self.assertIsNotNone(slice_obj)
        self.assertEqual(len(slice_obj), 0)

    def get_kernel_wrapper(self, *, actor: ABCActorMixin) -> KernelWrapper:
        wrapper = KernelWrapper(actor=actor, plugin=actor.get_plugin(), policy=actor.get_policy())
        return wrapper

    def prepare_actor(self):
        db = self.get_container_database()
        db.reset_db()
        actor = self.get_actor()
        db.remove_actor_database(actor_name=actor.get_name())
        db.add_actor(actor=actor)
        actor.actor_added()
        return actor

    def test_a_register_reservation(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_obj = SliceFactory.create(slice_id=ID(), name="test_slice")
        kernel.register_slice(slice_object=slice_obj)
        self.base_slices_count += 1

        for i in range(10):
            rid = ID()
            reservation = ClientReservationFactory.create(rid=rid, slice_object=slice_obj)
            kernel.register_reservation(reservation=reservation)

            self.assertIsNotNone(kernel.get_reservation(rid=rid))
            self.enforceReservationExistsInDatabase(db=db, rid=rid)

            failed = False

            try:
                kernel.register_reservation(reservation=reservation)
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.assertIsNotNone(kernel.get_reservation(rid=rid))
            self.enforceReservationExistsInDatabase(db=db, rid=rid)

    def test_b_register_slice_client(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slices = kernel.get_slices()
        self.assertIsNotNone(slices)
        self.assertEqual(0, len(slices))

        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))
            slice_obj.set_client()
            kernel.register_slice(slice_object=slice_obj)

            check = kernel.get_slice(slice_id=slice_obj.get_slice_id())
            self.assertIsNotNone(check)
            self.assertEqual(check, slice_obj)

            slices = kernel.get_slices()
            self.assertIsNotNone(slices)
            self.assertEqual(i + 1, len(slices))

            slices = kernel.get_client_slices()
            self.assertIsNotNone(slices)
            self.assertEqual(i + 1, len(slices))

            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

            slices = db.get_slices()
            self.assertIsNotNone(slice_obj)
            self.assertEqual(i + 1, len(slices))

            slices = db.get_slices(slc_type=[SliceTypes.ClientSlice, SliceTypes.BrokerClientSlice])
            self.assertIsNotNone(slice_obj)
            self.assertEqual(i + 1, len(slices))

            failed = False

            try:
                kernel.register_slice(slice_object=slice_obj)
            except Exception:
                failed = True

            self.assertTrue(failed)

            ss = kernel.get_slice(slice_id=slice_obj.get_slice_id())
            self.assertIsNotNone(ss)
            self.assertEqual(ss, slice_obj)

    def test_c_register_slice_inventory(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slices = kernel.get_slices()
        self.assertIsNotNone(slices)
        self.assertEqual(0, len(slices))

        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))
            slice_obj.set_inventory(value=True)
            kernel.register_slice(slice_object=slice_obj)

            check = kernel.get_slice(slice_id=slice_obj.get_slice_id())
            self.assertIsNotNone(check)
            self.assertEqual(check, slice_obj)

            slices = kernel.get_slices()
            self.assertIsNotNone(slices)
            self.assertEqual(i + 1, len(slices))

            slices = kernel.get_inventory_slices()
            self.assertIsNotNone(slices)
            self.assertEqual(i + 1, len(slices))

            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

            slices = db.get_slices()
            self.assertIsNotNone(slice_obj)
            self.assertEqual(i + 1, len(slices))

            slices = db.get_slices(slc_type=[SliceTypes.InventorySlice])
            self.assertIsNotNone(slice_obj)
            self.assertEqual(i + 1, len(slices))

            failed = False

            try:
                kernel.register_slice(slice_object=slice_obj)
            except Exception:
                failed = True

            self.assertTrue(failed)

            ss = kernel.get_slice(slice_id=slice_obj.get_slice_id())
            self.assertIsNotNone(ss)
            self.assertEqual(ss, slice_obj)

    def test_d_remove_slice_empty(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_list = []

        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))
            slice_obj.set_inventory(value=True)
            kernel.register_slice(slice_object=slice_obj)
            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())
            slice_list.append(slice_obj)

        for s in slice_list:
            kernel.remove_slice(slice_id=s.get_slice_id())

            check = kernel.get_slice(slice_id=s.get_slice_id())
            self.assertIsNone(check)
            self.enforceSliceNotInDatabase(db=db, slice_id=s.get_slice_id())

            kernel.remove_slice(slice_id=s.get_slice_id())
            self.enforceSliceNotInDatabase(db=db, slice_id=s.get_slice_id())

            kernel.register_slice(slice_object=s)
            self.enforceSliceExistsInDatabase(db=db, slice_id=s.get_slice_id())
            self.assertIsNotNone(kernel.get_slice(slice_id=s.get_slice_id()))

    def test_e_re_register_reservation(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_obj = SliceFactory.create(slice_id=ID(), name="test_slice")

        res_list = []
        for i in range(10):
            res = ClientReservationFactory.create(rid=ID())
            res.set_slice(slice_object=slice_obj)
            res_list.append(res)

            failed = False
            try:
                kernel.register_reservation(reservation=res)
            except Exception:
                failed = True

            self.assertTrue(failed)

            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationNotInDatabase(db=db, rid=res.get_reservation_id())
            self.assertIsNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
            self.enforceSliceNotInDatabase(db=db, slice_id=slice_obj.get_slice_id())

        kernel.register_slice(slice_object=slice_obj)

        for res in res_list:

            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationNotInDatabase(db=db, rid=res.get_reservation_id())

            failed = False
            try:
                kernel.re_register_reservation(reservation=res)
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationNotInDatabase(db=db, rid=res.get_reservation_id())

            kernel.register_reservation(reservation=res)

            self.assertIsNotNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

        kernel2 = self.get_kernel_wrapper(actor=actor)
        kernel2.re_register_slice(slice_object=slice_obj)

        for res in res_list:

            self.assertIsNone(kernel2.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

            failed = False
            try:
                kernel.re_register_reservation(reservation=res)
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.assertIsNone(kernel2.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

            kernel2.register_reservation(reservation=res)

            self.assertIsNotNone(kernel2.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

    def test_f_re_register_slice(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_list = []
        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))
            slice_list.append(slice_obj)

            failed = False

            try:
                kernel.re_register_slice(slice_object=slice_obj)
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.enforceSliceNotInDatabase(db=db, slice_id=slice_obj.get_slice_id())
            self.assertIsNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
            kernel.register_slice(slice_object=slice_obj)
            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

        kernel2 = self.get_kernel_wrapper(actor=actor)

        for s in slice_list:
            check = kernel2.get_slice(slice_id=s.get_slice_id())
            self.assertIsNone(check)
            self.enforceSliceExistsInDatabase(db=db, slice_id=s.get_slice_id())

            failed = False

            try:
                kernel2.register_slice(slice_object=s)
            except Exception:
                failed = True

            self.assertTrue(failed)

            check = kernel2.get_slice(slice_id=s.get_slice_id())
            self.assertIsNone(check)

            kernel2.re_register_slice(slice_object=s)

            check = kernel2.get_slice(slice_id=s.get_slice_id())
            self.assertIsNotNone(check)

            self.enforceSliceExistsInDatabase(db=db, slice_id=s.get_slice_id())

    def test_g_unregister_reservation(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_obj = SliceFactory.create(slice_id=ID(), name="testslice")
        kernel.register_slice(slice_object=slice_obj)

        res_list = []

        for i in range(10):
            res = ClientReservationFactory.create(rid=ID())
            res.set_slice(slice_object=slice_obj)
            res_list.append(res)

            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationNotInDatabase(db=db, rid=res.get_reservation_id())

            failed = False

            try:
                kernel.unregister_reservation(rid=res.get_reservation_id())
            except Exception:
                failed = True

            self.assertTrue(failed)

            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationNotInDatabase(db=db, rid=res.get_reservation_id())
            kernel.register_reservation(reservation=res)
            self.assertIsNotNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

        for res in res_list:
            self.assertIsNotNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())
            res.fail(message="forced")
            kernel.unregister_reservation(rid=res.get_reservation_id())
            self.assertIsNone(kernel.get_reservation(rid=res.get_reservation_id()))
            self.enforceReservationExistsInDatabase(db=db, rid=res.get_reservation_id())

        check = kernel.get_reservations(slice_id=slice_obj.get_slice_id())
        self.assertIsNotNone(check)
        self.assertEqual(0, len(check))

    def test_h_unregister_slice_empty(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slices = []

        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))
            slices.append(slice_obj)
            kernel.register_slice(slice_object=slice_obj)

        for s in slices:
            kernel.unregister_slice(slice_id=s.get_slice_id())
            check = kernel.get_slice(slice_id=s.get_slice_id())
            self.assertIsNone(check)
            self.enforceSliceExistsInDatabase(db=db, slice_id=s.get_slice_id())

            failed = False
            try:
                kernel.unregister_slice(slice_id=s.get_slice_id())
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.enforceSliceExistsInDatabase(db=db, slice_id=s.get_slice_id())

    def test_i_unregister_slice_full(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_obj = SliceFactory.create(slice_id=ID(), name="testslice")
        kernel.register_slice(slice_object=slice_obj)

        res_list = []

        for i in range(10):
            res = ClientReservationFactory.create(rid=ID())
            res.set_slice(slice_object=slice_obj)
            res_list.append(res)
            kernel.register_reservation(reservation=res)

        for r in res_list:
            self.assertIsNotNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

            failed = False

            try:
                kernel.unregister_slice(slice_id=slice_obj.get_slice_id())
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.assertIsNotNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
            self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

            r.fail(message="failed")
            kernel.unregister_reservation(rid=r.get_reservation_id())

        kernel.unregister_slice(slice_id=slice_obj.get_slice_id())
        self.assertIsNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
        self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())
        kernel.re_register_slice(slice_object=slice_obj)
        self.assertIsNotNone(kernel.get_slice(slice_id=slice_obj.get_slice_id()))
        self.enforceSliceExistsInDatabase(db=db, slice_id=slice_obj.get_slice_id())

    def test_j_register_reservation_error(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        slice_obj = SliceFactory.create(slice_id=ID(), name="test_slice")
        kernel.register_slice(slice_object=slice_obj)

        # Set the database to null to cause errors while registering slices.
        actor.get_plugin().set_database(db=None)

        for i in range(10):
            rid = ID()
            reservation = ClientReservationFactory.create(rid=rid, slice_object=slice_obj)
            failed = False

            try:
                kernel.register_reservation(reservation=reservation)
            except Exception:
                failed = True

            self.assertTrue(failed)
            self.assertIsNone(kernel.get_reservation(rid=rid))
            self.enforceReservationNotInDatabase(db=db, rid=rid)

    def test_k_register_slice_error(self):
        actor = self.prepare_actor()
        kernel = self.get_kernel_wrapper(actor=actor)
        db = actor.get_plugin().get_database()

        actor.get_plugin().set_database(db=None)

        for i in range(10):
            slice_obj = SliceFactory.create(slice_id=ID(), name="Slice:{}".format(i))

            failed = False

            try:
                kernel.register_slice(slice_object=slice_obj)
            except Exception:
                failed = True

            self.assertTrue(failed)

            check = kernel.get_slice(slice_id=slice_obj.get_slice_id())
            self.assertIsNone(check)
            self.enforceSliceNotInDatabase(db=db, slice_id=slice_obj.get_slice_id())
