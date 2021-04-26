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


class Notice:
    def __init__(self):
        self.notice_string = None

    def get_notice(self):
        """
        Get notice string
        :return:
        """
        return self.notice_string

    def add(self, *, msg: str, ex: Exception = None):
        """
        Addd message
        :param msg: message
        :param ex: exception
        :return:
        """
        if msg is None:
            return

        if self.notice_string is None:
            self.notice_string = msg
        else:
            self.notice_string += msg

        if ex is not None:
            self.notice_string += str(ex)

    def clear(self):
        """
        Clear the notice
        :return:
        """
        self.notice_string = None

    def is_empty(self) -> bool:
        """
        Returns true if empty; false otherwise
        :return: true if empty; false otherwise
        """
        return self.notice_string is None

    def __str__(self):
        if self.notice_string is not None:
            return self.notice_string
        return ""
