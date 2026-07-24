#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)
"""
Unit tests for the hardened pickle loader. No infrastructure required.
"""
import datetime
import pickle
import unittest
import uuid

from fabric_cf.actor.security.restricted_unpickler import restricted_loads


class _EvilOsSystem:
    def __reduce__(self):
        import os
        return (os.system, ("echo pwned",))


class _EvilEval:
    def __reduce__(self):
        return (eval, ("__import__('os').system('echo pwned')",))


class _EvilSubprocess:
    def __reduce__(self):
        import subprocess
        return (subprocess.check_output, (["id"],))


class TestRestrictedUnpickler(unittest.TestCase):
    def test_legit_builtins_roundtrip(self):
        for obj in ({"a": [1, 2], "b": ("x", 3.5)},
                    datetime.datetime(2026, 1, 2, 3, 4, 5),
                    uuid.uuid4(),
                    [1, "two", 3.0, None, True]):
            self.assertEqual(restricted_loads(pickle.dumps(obj)), obj)

    def test_legit_fim_sliver_roundtrip(self):
        from fim.slivers.capacities_labels import Capacities, Labels
        cap = Capacities(core=4, ram=16, disk=100)
        self.assertEqual(restricted_loads(pickle.dumps(cap)).core, 4)
        lab = Labels(vlan="200")
        self.assertEqual(restricted_loads(pickle.dumps(lab)).vlan, "200")

    def test_blocks_os_system_gadget(self):
        with self.assertRaises(pickle.UnpicklingError):
            restricted_loads(pickle.dumps(_EvilOsSystem()))

    def test_blocks_eval_gadget(self):
        with self.assertRaises(pickle.UnpicklingError):
            restricted_loads(pickle.dumps(_EvilEval()))

    def test_blocks_subprocess_gadget(self):
        with self.assertRaises(pickle.UnpicklingError):
            restricted_loads(pickle.dumps(_EvilSubprocess()))


if __name__ == "__main__":
    unittest.main()
