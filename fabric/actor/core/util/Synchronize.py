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
import _thread
import threading
from inspect import isfunction, isclass


def synchronized_with_attr(lock_name):
    def decorator(method):
        def synced_method(self, *args, **kws):
            lock = getattr(self, lock_name)
            with lock:
                return method(self, *args, **kws)

        return synced_method

    return decorator


def syncronized_with(lock):
    def synchronized_obj(obj):

        if isfunction(obj):

            obj.__lock__ = lock

            def func(*args, **kws):
                with lock:
                    obj(*args, **kws)

            return func

        elif isclass(obj):

            orig_init = obj.__init__

            def __init__(self, *args, **kws):
                self.__lock__ = lock
                orig_init(self, *args, **kws)

            obj.__init__ = __init__

            for key in obj.__dict__:
                val = obj.__dict__[key]
                if isfunction(val):
                    decorator = syncronized_with(lock)
                    setattr(obj, key, decorator(val))

            return obj

    return synchronized_obj


def synchronized(item):
    if isinstance(item, str):
        decorator = synchronized_with_attr(item)
        return decorator(item)

    if type(item) is _thread.LockType:
        decorator = syncronized_with(item)
        return decorator(item)

    else:
        new_lock = threading.Lock()
        decorator = syncronized_with(new_lock)
        return decorator(item)