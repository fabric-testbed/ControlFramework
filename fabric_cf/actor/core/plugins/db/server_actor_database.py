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
import pickle

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.plugins.db.client_database import ClientDatabase
from fabric_cf.actor.core.util.client import Client
from fabric_cf.actor.core.util.id import ID


class ServerActorDatabase(ActorDatabase, ClientDatabase):
    def add_client(self, *, client: Client):
        try:
            self.lock.acquire()
            properties = pickle.dumps(client)
            self.db.add_client(act_id=self.actor_id, clt_name=client.get_name(), clt_guid=str(client.get_guid()),
                               properties=properties)
        finally:
            self.lock.release()

    def update_client(self, *, client: Client):
        try:
            self.lock.acquire()
            properties = pickle.dumps(client)
            self.db.update_client(act_id=self.actor_id, clt_name=client.get_name(), properties=properties)
        finally:
            self.lock.release()

    def remove_client(self, *, guid: ID):
        try:
            self.lock.acquire()
            self.db.remove_client_by_guid(act_id=self.actor_id, clt_guid=str(guid))
        finally:
            self.lock.release()

    def get_client(self, *, guid: ID) -> dict:
        result = None
        try:
            self.lock.acquire()
            result = self.db.get_client_by_guid(act_id=self.actor_id, clt_guid=str(guid))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return result

    def get_clients(self) -> list:
        result = None
        try:
            self.lock.acquire()
            result = self.db.get_clients(act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return result
