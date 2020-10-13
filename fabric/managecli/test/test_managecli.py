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

from click.testing import CliRunner
from fabric.managecli import managecli


class ManageCliTest(unittest.TestCase):
    am_slice_id = 'test_am'
    broker_slice_id = 'test_broker'
    am_res_id = 'test_res_am'
    broker_res_id = 'test_res_broker'

    def test_a_claim_resources(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'claim', '--broker', 'fabric-broker', '--am',
                                                 'fabric-site-am'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Code") == -1)

    def test_b_claim_resources_by_rid(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'claim', '--broker', 'fabric-broker', '--am',
                                                 'fabric-site-am', '--rid', '63deb5b6-e291-4ed5-9d9f-fc58a70605f4'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Code") == -1)

    def test_c_reclaim_resources_by_rid(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'reclaim', '--broker', 'fabric-broker', '--am',
                                                 'fabric-site-am', '--rid', '63deb5b6-e291-4ed5-9d9f-fc58a70605f4'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Code") == -1)

    def test_d_get_slices_am(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-site-am'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Slice Guid") != -1)

    def test_e_get_slices_broker(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-broker'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Slice Guid") != -1)

    def test_f_get_slice_am(self):
        print("Using slice_id:{}".format(self.am_slice_id))
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-site-am', '--sliceid', ManageCliTest.am_slice_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("ErrorNoSuchSlice") != -1)

    def test_g_get_slice_broker(self):
        print("Using slice_id:{}".format(self.broker_slice_id))
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-broker', '--sliceid', ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("ErrorNoSuchSlice") != -1)

    def test_h_get_reservations_broker(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-broker'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Reservation ID") != -1)

    def test_i_get_reservations_am(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-site-am'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Reservation ID") != -1)

    def test_j_get_reservation(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-broker', '--rid', ManageCliTest.broker_res_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("ErrorNoSuchReservation") != -1)

    def test_k_get_reservation(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-site-am', '--rid', ManageCliTest.am_res_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("ErrorNoSuchReservation") != -1)

    def test_l_close_reservation(self):
        runner = CliRunner()
        #result = runner.invoke(managecli.managecli, ['manage', 'closereservation', '--actor', 'fabric-broker', '--rid',
        #                                             ManageCliTest.am_res_id])
        result = runner.invoke(managecli.managecli, ['manage', 'closereservation', '--actor', 'fabric-broker', '--rid',
                                                     '1e5c5119-3532-49e8-ab65-05296fd00602'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find('False') != -1)

    def test_m_close_slice(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'closeslice', '--actor', 'fabric-broker', '--sliceid',
                                                     ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find('False') != -1)

    def test_n_remove_reservation(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'removereservation', '--actor', 'fabric-broker', '--rid',
                                                     ManageCliTest.am_res_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find('True') != -1)

    def test_o_remove_slice(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'removeslice', '--actor', 'fabric-broker', '--sliceid',
                                                     ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find('True') != -1)

    def test_p_claim_resources(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'claimdelegation', '--broker', 'fabric-broker', '--am',
                                                 'fabric-site-am'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Code") == -1)

    def test_q_get_delegations_am(self):
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'delegations', '--actor', 'fabric-site-am'])
        print("Result: {}".format(result.output))
        self.assertTrue(result.output.find("Delegation ID") != -1)