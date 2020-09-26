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
from functools import total_ordering


class ID:
    def __init__(self, *, id: str = None):
        if id is None:
            self.id = uuid.uuid4().__str__()
        else:
            self.id = id

    def __str__(self):
        return str(self.id)

    def print(self):
        print(str(self.id))

    def __hash__(self):
        return hash(self.id)

    def __lt__(self, other):
        if not isinstance(other, ID):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.id < other.id

    def __eq__(self, other):
        if not isinstance(other, ID):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.id == other.id


def main():
    id = ID()
    id.print()
    print(id.__hash__())
    id1 = ID(id="manager")
    id1.print()
    print(id1.__hash__())


if __name__ == "__main__":
    main()
