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


class FreeAllocatedSet:
    """
    FreeAllocatedSet is a simple data structure that maintains two sets: free and allocated.
    The structure can be used to track free and allocated items. Use add_inventory to
    add inventory items, allocate to allocate an item, and free to free an item.
    """
    def __init__(self):
        # Free set.
        self.free_set = set()
        # Allocated set.
        self.allocated = set()

    def add_inventory(self, *, item):
        """
        Adds an inventory item to the set.
        @param item item to add
        """
        if item is None:
            raise Exception("item cannot be null")

        if item in self.allocated:
            raise Exception("item is already in allocated")

        self.free_set.add(item)

    def allocate(self, *, tag=None, config_tag: bool = None, count: int = None):
        """
        Allocates an item from the set.
        @param tag tag
        @param config_tag config_tag
        @param count number of units to allocate.

        @return an allocated item, or null if the set does not have a free item
        """
        item = None

        if len(self.free_set) > 0:
            if tag is None and config_tag is None and count is None:
                item = self.free_set.pop()
                self.allocated.add(item)

            elif tag is not None and config_tag is not None:
                if config_tag:
                    item = tag
                    if item not in self.free_set:
                        raise Exception("item is already in allocated: {}".format(item))
                else:
                    item = self.free_set.pop()
                self.allocated.add(item)
        return item

    def allocate_count(self, *, count: int):
        item = None
        if len(self.free_set) > 0:
            item = []
            for i in range(count):
                val = self.free_set.pop()
                self.allocated.add(val)
                item.append(val)
        return item

    def free(self, *, item=None, count: int = None):
        if item is None and count is None:
            if len(self.allocated) == 0:
                raise Exception("no items have been allocated")

            item = self.allocated.pop()
            self.free_set.add(item)
        elif item is not None:
            if item not in self.allocated:
                raise Exception("item has not been allocated")
            if item in self.free_set:
                raise Exception("item has already been freed")

            self.allocated.remove(item)
            self.free_set.add(item)
        elif count is not None:
            for i in range(count):
                if len(self.allocated) == 0:
                    raise Exception("no items have been allocated")

                item = self.allocated.pop()
                self.free_set.add(item)

    def free_list(self, *, items: list):
        for i in items:
            self.free(item=i)

    def get_free(self) -> int:
        return len(self.free_set)

    def get_allocated(self) -> int:
        return len(self.allocated)

    def size(self):
        return self.get_free() + self.get_allocated()

    def __str__(self):
        return "Free: {}\nAllocated: {}".format(self.free_set, self.allocated)
