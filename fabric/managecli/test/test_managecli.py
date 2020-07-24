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
        print("=======Begin test1")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'claim', '--broker', 'fabric-broker', '--am',
                                                     'fabric-vm-am'])
        print("Result: {}".format(result.output))
        print("=======End test1")

    def test_b_get_slices_am(self):
        print("=======Begin test2")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-vm-am'])
        print("Result: {}".format(result.output))
        output_str = result.output.split('\n')
        for s in output_str:
            if s.find('Slice Name') != -1:
                ManageCliTest.am_slice_id = s.split('Guid: ')[1]
                break
        print("=======End test2")

    def test_c_get_slices_broker(self):
        print("=======Begin test3")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slices', '--actor', 'fabric-broker'])
        print("Result: {}".format(result.output))
        output_str = result.output.split('\n')
        for s in output_str:
            if s.find('Slice Name') != -1:
                ManageCliTest.broker_slice_id = s.split('Guid: ')[1]
                break
        print("=======End test3")

    def test_d_get_slice_am(self):
        print("=======Begin test4")
        print("Using slice_id:{}".format(self.am_slice_id))
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slice', '--actor', 'fabric-vm-am', '--sliceid', ManageCliTest.am_slice_id])
        print("Result: {}".format(result.output))
        print("=======End test4")

    def test_e_get_slice_broker(self):
        print("=======Begin test5")
        print("Using slice_id:{}".format(self.broker_slice_id))
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'slice', '--actor', 'fabric-broker', '--sliceid', ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        print("=======End test5")

    def test_f_get_reservations_broker(self):
        print("=======Begin test6")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-broker'])
        print("Result: {}".format(result.output))
        output_str = result.output.split('\n')
        for r in output_str:
            if r.find('Reservation ID') != -1:
                ManageCliTest.broker_res_id = r.split(' ')[2]
                break
        print("=======End test6")

    def test_g_get_reservations_am(self):
        print("=======Begin test7")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservations', '--actor', 'fabric-vm-am'])
        print("Result: {}".format(result.output))
        output_str = result.output.split('\n')
        for r in output_str:
            if r.find('Reservation ID') != -1:
                ManageCliTest.am_res_id = r.split(' ')[2]
                break
        print("=======End test7")

    def test_h_get_reservation(self):
        print("=======Begin test8")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservation', '--actor', 'fabric-broker', '--rid', ManageCliTest.broker_res_id])
        print("Result: {}".format(result.output))
        print("=======End test8")

    def test_i_get_reservation(self):
        print("=======Begin test9")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['show', 'reservation', '--actor', 'fabric-vm-am', '--rid', ManageCliTest.am_res_id])
        print("Result: {}".format(result.output))
        print("=======End test9")

    def test_j_close_reservation(self):
        print("=======Begin test10")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'closereservation', '--actor', 'fabric-broker', '--rid',
                                                     ManageCliTest.am_res_id])
        print("Result: {}".format(result.output))
        print("=======End test10")

    def test_k_close_slice(self):
        print("=======Begin test11")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'closeslice', '--actor', 'fabric-broker', '--sliceid',
                                                     ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        print("=======End test11")

    def test_l_remove_reservation(self):
        print("=======Begin test12")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'removereservation', '--actor', 'fabric-broker', '--rid',
                                                     ManageCliTest.am_res_id])
        print("Result: {}".format(result.output))
        print("=======End test12")

    def test_m_remove_slice(self):
        print("=======Begin test13")
        runner = CliRunner()
        result = runner.invoke(managecli.managecli, ['manage', 'removeslice', '--actor', 'fabric-broker', '--sliceid',
                                                     ManageCliTest.broker_slice_id])
        print("Result: {}".format(result.output))
        print("=======End test13")