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

GET_METHOD = 'get'
POST_METHOD = 'post'
PUT_METHOD = 'put'
DELETE_METHOD = 'delete'

RESOURCES_PATH = '/resources'
PORTAL_RESOURCES_PATH = '/portalresources'

SLICES_CREATE_PATH = '/slices/create'
SLICES_DELETE_PATH = '/slices/delete'
SLICES_GET_PATH = '/slices'
SLICES_GET_SLICE_ID_PATH = '/slices/{sliceID}'
SLICES_RENEW_PATH = '/slices/renew/{sliceID}'
SLICE_STATUS_SLICE_ID_PATH = '/slices/status/{sliceID}'


SLIVERS_GET_PATH = '/slivers'
SLIVERS_GET_SLIVER_ID_PATH = '/slivers/{sliverID}'
SLIVERS_STATUS_SLIVER_ID_PATH = '/slivers/status/{sliverID}'

VERSIONS_PATH = '/version'

INTERNAL_SERVER_ERROR = 500
BAD_REQUEST = 400
HTTP_OK = 200