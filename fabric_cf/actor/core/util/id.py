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
import uuid


class ID:
    def __init__(self, *, uid: str = None):
        if uid is None:
            self.uid = uuid.uuid4().__str__()
        else:
            self.uid = uid

    def __str__(self):
        return str(self.uid)

    def print(self):
        """
        Print
        :return:
        """
        print(str(self.uid))

    def __hash__(self):
        return hash(self.uid)

    def __lt__(self, other):
        if not isinstance(other, ID):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.uid < other.uid

    def __eq__(self, other):
        if not isinstance(other, ID):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.uid == other.uid


def main():
    uid = ID()
    uid.print()
    print(uid.__hash__())
    uid1 = ID(uid="manager")
    uid1.print()
    print(uid1.__hash__())


if __name__ == "__main__":
    main()
