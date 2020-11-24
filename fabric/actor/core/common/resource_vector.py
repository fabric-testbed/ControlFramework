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
import array as arr


class ResourceVector:
    """
    Resource Vector
    """
    def __init__(self, *, dimensions: int):
        self.vector = arr.array('L')
        for i in range(dimensions):
            self.vector.append(0)

    def _enforce_compatibility(self, *, other):
        """
        Enforce compatibility
        @param other other
        """
        if other is None:
            raise Exception("other cannot be null")

        if not isinstance(other, ResourceVector):
            raise Exception("Invalid argument")

        if len(other.vector) != len(self.vector):
            raise Exception("mismatched dimensions")

    def add(self, *, other):
        """
        Adds the specified vector to this instance.
        @params other : vector to add
        """
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            self.vector[i] += other.vector[i]

    def subtract(self, *, other):
        """
        Subtract the specified vector to this instance.
        @params other : vector to subtract
        """
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            self.vector[i] -= other.vector[i]

    def multiply(self, *, times: int):
        """
        multiply the specified vector to this instance.
        @params other : vector to multiply
        """
        for i in range(len(self.vector)):
            self.vector[i] *= times

    def subtract_me_from_and_update_me(self, *, other):
        """
        Subtracts the specified vector from this instance.
        @params other : vector to subtract
        """
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            self.vector[i] = other.vector[i] - self.vector[i]

    def is_positive(self):
        for i in range(len(self.vector)):
            if self.vector[i] <= 0:
                return False
        return True

    def contains(self, *, other):
        """
        Checks if this instance contains the specified vector.
        @params other : vector to check
        @returns true or false
        """
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            if self.vector[i] > other.vector[i]:
                return False
        return True

    def contains_or_equals(self, *, other):
        """
        Checks if this instance contains the specified vector.
        @params other : vector to check
        @returns true or false
        """
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            if self.vector[i] >= other.vector[i]:
                return False
        return True

    def will_have_negative_dimensions(self, *, other):
        self._enforce_compatibility(other=other)
        for i in range(len(other.vector)):
            if self.vector[i] - other.vector[i] < 0:
                return True
        return False

    def will_overflow_on_subtract(self, *, to_subtract, limit):
        self._enforce_compatibility(other=to_subtract)
        self._enforce_compatibility(other=limit)
        for i in range(len(to_subtract.vector)):

            if self.vector[i] - to_subtract.vector[i] > limit.vector[i]:
                return True
        return False

    def __str__(self):
        result = "("
        for i in range(len(self.vector)):
            result += ",{}".format(self.vector[i])
        result += ")"
        return result
