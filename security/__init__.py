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

import sys
import configparser

ConfDir = '/etc/actor'
ConfFile = 'config'

LogDir = '/var/log/actor'
LogFile = 'actor.log'
LogLevel = 'DEBUG'
LogRetain = '5'
LogFileSize = '5000000'

OauthUserUri = 'https://cilogon.org/oauth2/userinfo'
OauthJwksUri = 'https://cilogon.org/oauth2/certs'
LOGGER = 'actor_logger'

CONFIG = configparser.ConfigParser()
CONFIG.add_section('oauth')
CONFIG.add_section('logging')

CONFIG.set('logging', 'log-directory', LogDir)
CONFIG.set('logging', 'log-file', LogFile)
CONFIG.set('logging', 'log-level', LogLevel)
CONFIG.set('logging', 'log-retain', LogRetain)
CONFIG.set('logging', 'log-file-size', LogFileSize)
CONFIG.set('oauth', 'oauth-user-url', OauthUserUri)
CONFIG.set('oauth', 'oauth-jwks-url', OauthJwksUri)

# Now, attempt to read in the configuration file.
config_file = ConfDir + '/' + ConfFile
try:
    files_read = CONFIG.read(config_file)
    if len(files_read) == 0:
        sys.stderr.write('Configuration file could not be read; ' +
                 'proceeding with default settings.')
except Exception as e:
    raise RuntimeError('Unable to parse configuration file')
