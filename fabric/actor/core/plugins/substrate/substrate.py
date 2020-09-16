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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric.actor.core.core.actor import Actor
    from fabric.actor.core.apis.i_database import IDatabase
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.core.unit import Unit
    from fabric.actor.core.plugins.config.config_token import ConfigToken

from fabric.actor.core.plugins.config.config import Config
from fabric.actor.core.apis.i_substrate_database import ISubstrateDatabase
from fabric.actor.core.plugins.base_plugin import BasePlugin
from fabric.actor.core.apis.i_substrate import ISubstrate
from fabric.actor.core.util.prop_list import PropList


class Substrate(BasePlugin, ISubstrate):
    def __init__(self, actor: Actor, db: ISubstrateDatabase, config: Config):
        super().__init__(actor, db, config)

    def __getstate__(self):
        state = self.__dict__.copy()
        state['actor_id'] = self.actor.get_guid()
        del state['logger']
        del state['ticket_factory']
        del state['actor']
        del state['initialized']

        return state

    def __setstate__(self, state):
        actor_id = state['actor_id']
        # TODO fetch actor via actor_id
        del state['actor_id']
        self.__dict__.update(state)

    def initialize(self):
        super().initialize()
        if not isinstance(self.db, ISubstrateDatabase):
            raise Exception("Substrate database class must implement ISubstrateDatabase")

    def transfer_in(self, reservation: IReservation, unit: Unit):
        try:
            # record the node in the database
            self.get_substrate_database().add_unit(unit)
            # prepare the transfer
            self.prepare_transfer_in(reservation, unit)
            # update the unit database record
            # since prepareTransferIn may have added new properties.
            self.db.update_unit(unit)
            # perform the node configuration
            self.do_transfer_in(reservation, unit)
        except Exception as e:
            self.fail_and_update(unit, "transferIn error", e)

    def transfer_out(self, reservation: IReservation, unit: Unit):
        try:
            # prepare the transfer out
            self.prepare_transfer_out(reservation, unit)
            # update the unit database record
            self.get_substrate_database().update_unit(unit)
            # perform the node configuration
            self.do_transfer_out(reservation, unit)
        except Exception as e:
            self.fail_and_update(unit, "transferOut error", e)

    def modify(self, reservation: IReservation, unit: Unit):
        try:
            # prepare the transfer out
            self.prepare_modify(reservation, unit)
            # update the unit database record
            self.db.update_unit(unit)
            # perform the node configuration
            self.do_modify(reservation, unit)
        except Exception as e:
            self.fail_and_update(unit, "modify error", e)

    def prepare_transfer_in(self, reservation: IReservation, unit: Unit):
        """
        Performs additional setup operations before configuring the unit. This is
        an optional step of the transfer in process and can be used to set custom
        properties on the reservation/unit objects before invoking the
        configuration subsystem. For example, if the policy did not allocate an
        IP address for the unit, but an IP address is required to configure the
        unit, the IP address can be allocated in this function.
        @param reservation reservation containing the unit
        @param unit unit to prepare
        @throws Exception in case of error
        """
        return

    def prepare_transfer_out(self, reservation: IReservation, unit: Unit):
        """
        Prepares the unit for transfer out. Note: resources assigned in
        prepareTransferIn cannot be released yet, since they are still in use.
        The resources can be released only when the transferOut operation
        completes. See {@link #processLeaveComplete(Object, Properties)}.
        @param reservation reservation containing the unit
        @param unit unit to prepare
        @throws Exception in case of error
        """
        return

    def prepare_modify(self, reservation: IReservation, unit: Unit):
        """
        Prepares the unit for modification.
        @param reservation reservation containing the unit
        @param unit unit to prepare
        @throws Exception in case of error
        """
        return

    def get_config_properties_from_reservation(self, reservation: IReservation, unit: Unit) -> dict:
        temp = reservation.get_resources().get_local_properties()
        temp = PropList.merge_properties(reservation.get_slice().get_local_properties(), temp)

        if self.is_site_authority():
            temp = PropList.merge_properties(reservation.get_resources().get_config_properties(), temp)
            temp = PropList.merge_properties(reservation.get_slice().get_config_properties(), temp)

            if reservation.get_requested_resources() is not None:
                ticket = reservation.get_requested_resources().get_resources()
                temp = PropList.merge_properties(ticket.get_properties(), temp)
                rticket = ticket.get_ticket()
                temp = PropList.merge_properties(rticket.get_properties(), temp)

        temp = PropList.merge_properties(unit.get_properties(), temp)

        return temp

    def do_transfer_in(self, reservation: IReservation, unit: Unit):
        prop = self.get_config_properties_from_reservation(reservation, unit)
        self.config.join(unit, prop)

    def do_transfer_out(self, reservation: IReservation, unit: Unit):
        prop = self.get_config_properties_from_reservation(reservation, unit)
        self.config.leave(unit, prop)

    def do_modify(self, reservation: IReservation, unit: Unit):
        prop = self.get_config_properties_from_reservation(reservation, unit)
        self.config.modify(unit, prop)

    def fail_and_update(self, unit: Unit, message: str, e: Exception):
        self.logger.error(message)
        self.logger.error(str(e))

        try:
            unit.fail(message, e)
            self.db.update_unit(unit)
        except Exception as e:
            self.logger.error("could not update unit in database")
            self.logger.error(e)

    def fail_no_update(self, unit: Unit, message: str, e: Exception = None):
        self.logger.error(message)
        if e:
            self.logger.error(e)
        unit.fail(message, e)

    def fail_modify_no_update(self, unit: Unit, message: str, e: Exception = None):
        self.logger.error(message)
        if e:
            self.logger.error(e)
        unit.fail_on_modify(message, e)

    def merge_unit_properties(self, unit: Unit, properties: dict):
        # TODO
        return

    def process_join_complete(self, token: ConfigToken, properties: dict):
        self.logger.debug("Join")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise Exception("This actor cannot receive updates")

        sequence = Config.get_action_sequence_number(properties)
        notice = None
        # TODO synchronized on token
        if sequence != token.get_sequence():
            self.logger.warning("(join complete) sequences mismatch: incoming ({}) local: ({}). Ignoring event.".
                                format(sequence, token.get_sequence()))
            return
        else:
            self.logger.debug("(join complete) incoming ({}) local: ({})".format(sequence, token.get_sequence()))

        result = Config.get_result_code(properties)
        msg = Config.get_exception_message(properties)
        if msg is None:
            msg = Config.get_result_code_message(properties)

        if result == 0:
            self.logger.debug("join code 0 (success)")
            self.merge_unit_properties(token, properties)
            token.activate()

        elif result == -1:
            self.logger.debug("join code -1 with message: {}".format(msg))
            notice = "Exception during join for unit: {} {}".format(token.get_id(), msg)
            self.fail_no_update(token, notice)

        else:
            self.logger.debug("join code {} with message: {}".format(result, msg))
            notice = "Error code= {} during join for unit: {} with message: {}".format(result, token.get_id(), msg)
            self.fail_no_update(token, notice)

        try:
            self.get_substrate_database().update_unit(token)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process join complete")

    def process_leave_complete(self, token: ConfigToken, properties: dict):
        self.logger.debug("Leave")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise Exception("This actor cannot receive updates")

        sequence = Config.get_action_sequence_number(properties)
        notice = None
        # TODO synchronized on token
        if sequence != token.get_sequence():
            self.logger.warning("(leave complete) sequences mismatch: incoming ({}) local: ({}). Ignoring event.".format(sequence, token.get_sequence()))
            return
        else:
            self.logger.debug("(leave complete) incoming ({}) local: ({})".format(sequence, token.get_sequence()))

        result = Config.get_result_code(properties)
        msg = Config.get_exception_message(properties)
        if msg is None:
            msg = Config.get_result_code_message(properties)

        if result == 0:
            self.logger.debug("leave code 0 (success)")
            self.merge_unit_properties(token, properties)
            token.close()

        elif result == -1:
            self.logger.debug("leave code -1 with message: {}".format(msg))
            notice = "Exception during join for unit: {} {}".format(token.get_id(), msg)
            self.fail_no_update(token, notice)

        else:
            self.logger.debug("leave code {} with message: {}".format(result, msg))
            notice = "Error code= {} during join for unit: {} with message: {}".format(result, token.get_id(), msg)
            self.fail_no_update(token, notice)

        try:
            self.get_substrate_database().update_unit(token)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process leave complete")

    def process_modify_complete(self, token: ConfigToken, properties: dict):
        self.logger.debug("Modify")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise Exception("This actor cannot receive updates")

        sequence = Config.get_action_sequence_number(properties)
        notice = None
        # TODO synchronized on token
        if sequence != token.get_sequence():
            self.logger.warning("(modify complete) sequences mismatch: incoming ({}) local: ({}). Ignoring event.".format(sequence, token.get_sequence()))
            return
        else:
            self.logger.debug("(modify complete) incoming ({}) local: ({})".format(sequence, token.get_sequence()))

        # TODO properties

        result = Config.get_result_code(properties)
        msg = Config.get_exception_message(properties)
        if msg is None:
            msg = Config.get_result_code_message(properties)

        if result == 0:
            self.logger.debug("modify code 0 (success)")
            self.merge_unit_properties(token, properties)
            token.complete_modify()

        elif result == -1:
            self.logger.debug("modify code -1 with message: {}".format(msg))
            notice = "Exception during join for unit: {} {}".format(token.get_id(), msg)
            self.fail_modify_no_update(token, notice)

        else:
            self.logger.debug("modify code {} with message: {}".format(result, msg))
            notice = "Error code= {} during join for unit: {} with message: {}".format(result, token.get_id(), msg)
            self.fail_modify_no_update(token, notice)

        try:
            self.get_substrate_database().update_unit(token)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process modify complete")

    def get_substrate_database(self) -> ISubstrateDatabase:
        return self.db

    def set_database(self, db: IDatabase):
        if db is not None and not isinstance(db, ISubstrateDatabase):
            raise Exception("db must implement ISubstrateDatabase")

        super().set_database(db)

    def update_props(self, reservation: IReservation, unit: Unit):
        try:
            self.get_substrate_database().update_unit(unit)
        except Exception as e:
            self.fail_and_update(unit, "update properties error", e)
