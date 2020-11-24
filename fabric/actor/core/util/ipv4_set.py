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
from __future__ import annotations
import ipaddress


class IPv4Set:
    EntrySeparator = ","
    SubnetMark = "/"
    RangeMark = "-"

    def __init__(self, *, ip_list: str = None):
        self.free_set = set()
        self.allocated = set()

        if ip_list:
            self.add(ip_list=ip_list)

    def add(self, *, ip_list: str):
        for token in ip_list.split(self.EntrySeparator):
            if token.find(self.SubnetMark) > -1:
                self.process_subnet(token=token)
            elif token.find(self.RangeMark) > -1:
                self.process_range(token=token)
            else:
                self.process_single(token=token)

    def process_subnet(self, *, token: str):
        tokens = token.split(self.SubnetMark)
        if len(tokens) != 2:
            raise Exception("Invalid subnet: {}".find(token))

        base = self.to_IP4(ip=tokens[0])

        size = int(tokens[1])

        if size < 16 or size > 32:
            raise Exception("Invalid subnet size: {}".format(size))

        network = ipaddress.IPv4Network(token, False)
        all = list(network.hosts())
        last = all[-1]
        start = base
        end = int(last)

        while start <= end:
            self.free_set.add(start)
            start += 1

    def process_range(self, *, token: str):
        tokens = token.split(self.RangeMark)
        if len(tokens) != 2:
            raise Exception("Invalid range: {}".find(token))

        start = str(self.pad_if_needed(token=tokens[0]))
        start_ip = self.to_IP4(ip=start)
        end = tokens[1]

        if end.find(".") == -1:
            end = start[0:start.rfind(".")] + "." + end

        end_ip = self.to_IP4(ip=end)

        size = end_ip - start_ip + 1

        if size < 0 | size > 65536:
            raise Exception("Range must be positive and less than 65536")

        for i in range(size):
            self.free_set.add(start_ip + i)

    def pad_if_needed(self, *, token: str):
        index = 0
        count = 0
        index = token.find(".")

        while index != -1:
            count += 1
            index = token.find(".", index+1)

        for i in range(3-count):
            token = token + ".0"

        return token

    def process_single(self, *, token: str):
        self.free_set.add(self.to_IP4(ip=token))

    def allocate(self):
        item = self.free_set.pop()
        self.allocated.add(item)
        ret_val = self.int_to_IP4(ip=item)
        return ret_val

    def free(self, *, ip: str):
        val = self.to_IP4(ip=ip)
        self.allocated.remove(val)
        self.free_set.add(val)

    def reserve(self, *, ip: str):
        val = self.to_IP4(ip=ip)
        self.free_set.remove(val)
        self.allocated.add(val)

    def get_free_count(self) -> int:
        return len(self.free_set)

    def is_free(self, *, ip: int) -> bool:
        if ip in self.free_set:
            return True
        return False

    def is_allocated(self, *, ip: int) -> bool:
        if ip in self.allocated:
            return True
        return False

    def to_IP4(self, *, ip: str):
        tokens = ip.split(".")
        if len(tokens) != 4:
            raise Exception("Invalid ip address: {}".format(ip))

        result = 0
        try:
            result = int(ipaddress.ip_address(ip))
        except Exception as e:
            raise Exception("Invalid ip address: {}".format(ip))

        return result

    def int_to_IP4(self, *, ip: int):
        result = None
        try:
            result = ipaddress.ip_address(ip).__str__()
        except Exception as e:
            raise Exception("Invalid ip address: {}".format(ip))

        return result

    def __str__(self):
        return "IP Free:{}\nIP Allocated:{}".format(self.free_set, self.allocated)



