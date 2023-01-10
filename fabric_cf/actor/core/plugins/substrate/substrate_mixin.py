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

import traceback
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PluginException
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
from fabric_cf.actor.core.apis.abc_substrate_database import ABCSubstrateDatabase
from fabric_cf.actor.core.plugins.base_plugin import BasePlugin
from fabric_cf.actor.core.apis.abc_substrate import ABCSubstrate

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_database import ABCDatabase
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.core.unit import Unit
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken


class SubstrateMixin(BasePlugin, ABCSubstrate):
    def __init__(self, *, actor: ActorMixin, db: ABCSubstrateDatabase, handler_processor: HandlerProcessor):
        super().__init__(actor=actor, db=db, handler_processor=handler_processor)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['initialized']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.resource_delegation_factory = None
        self.actor = None
        self.initialized = False

    def initialize(self):
        super().initialize()
        if not isinstance(self.db, ABCSubstrateDatabase):
            raise PluginException("Substrate database class must implement ISubstrateDatabase")

    def transfer_in(self, *, reservation: ABCReservationMixin, unit: Unit):
        try:
            # record the node in the database
            self.get_substrate_database().add_unit(u=unit)
            # prepare the transfer
            self.prepare_transfer_in(reservation=reservation, unit=unit)
            # update the unit database record
            # since prepareTransferIn may have added new properties.
            self.db.update_unit(u=unit)
            # perform the node configuration
            self.do_transfer_in(reservation=reservation, unit=unit)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.fail_and_update(unit=unit, message="transferIn error", e=e)

    def transfer_out(self, *, reservation: ABCReservationMixin, unit: Unit):
        try:
            # prepare the transfer out
            self.prepare_transfer_out(reservation=reservation, unit=unit)
            # update the unit database record
            self.get_substrate_database().update_unit(u=unit)
            # perform the node configuration
            self.do_transfer_out(reservation=reservation, unit=unit)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.fail_and_update(unit=unit, message="transferOut error", e=e)

    def modify(self, *, reservation: ABCReservationMixin, unit: Unit):
        try:
            # prepare the transfer out
            self.prepare_modify(reservation=reservation, unit=unit)
            # update the unit database record
            self.db.update_unit(u=unit)
            # perform the node configuration
            self.do_modify(reservation=reservation, unit=unit)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.fail_and_update(unit=unit, message="modify error", e=e)

    def prepare_transfer_in(self, *, reservation: ABCReservationMixin, unit: Unit):
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

    def prepare_transfer_out(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Prepares the unit for transfer out. Note: resources assigned in
        prepareTransferIn cannot be released yet, since they are still in use.
        The resources can be released only when the transferOut operation
        completes. See {@link #processLeaveComplete(Object, Properties)}.
        @param reservation reservation containing the unit
        @param unit unit to prepare
        @throws Exception in case of error
        """

    def prepare_modify(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Prepares the unit for modification.
        @param reservation reservation containing the unit
        @param unit unit to prepare
        @throws Exception in case of error
        """
    def do_transfer_in(self, *, reservation: ABCReservationMixin, unit: Unit):
        self.handler_processor.create(unit=unit)

    def do_transfer_out(self, *, reservation: ABCReservationMixin, unit: Unit):
        self.handler_processor.delete(unit=unit)

    def do_modify(self, *, reservation: ABCReservationMixin, unit: Unit):
        self.handler_processor.modify(unit=unit)

    def fail_and_update(self, *, unit: Unit, message: str, e: Exception):
        self.logger.error(message)
        self.logger.error(str(e))

        try:
            unit.fail(message=message, exception=e)
            self.db.update_unit(u=unit)
        except Exception as ex:
            self.logger.error("could not update unit in database")
            self.logger.error(ex)

    def fail_no_update(self, *, unit: Unit, message: str, e: Exception = None):
        self.logger.error(message)
        if e:
            self.logger.error(e)
        unit.fail(message=message, exception=e)

    def fail_modify_no_update(self, *, unit: Unit, message: str, e: Exception = None):
        self.logger.error(message)
        if e:
            self.logger.error(e)
        if unit.is_network_service():
            unit.fail(message=message, exception=e)
        else:
            unit.fail_on_modify(message=message, exception=e)

    def merge_unit_properties(self, *, unit: Unit, properties: dict):
        # TODO
        return

    def process_create_complete(self, *, unit: ConfigToken, properties: dict):
        self.logger.debug("Create")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise PluginException(Constants.INVALID_ACTOR_STATE)

        sequence = HandlerProcessor.get_action_sequence_number(properties=properties)
        notice = None
        # TODO synchronized on token
        if sequence != unit.get_sequence():
            self.logger.warning("(create complete) sequences mismatch: incoming ({}) local: ({}). Ignoring event.".
                                format(sequence, unit.get_sequence()))
            return
        else:
            self.logger.debug("(create complete) incoming ({}) local: ({})".format(sequence, unit.get_sequence()))

        result = HandlerProcessor.get_result_code(properties=properties)
        msg = HandlerProcessor.get_exception_message(properties=properties)
        if msg is None:
            msg = HandlerProcessor.get_result_code_message(properties=properties)

        if result == 0:
            self.logger.debug("create code 0 (success)")
            self.merge_unit_properties(unit=unit, properties=properties)
            unit.activate()

        elif result == -1:
            self.logger.debug("create code -1 with message: {}".format(msg))
            notice = "Exception during create for unit: {} {}".format(unit.get_id(), msg)
            self.fail_no_update(unit=unit, message=notice)

        else:
            self.logger.debug("create code {} with message: {}".format(result, msg))
            notice = "Error code= {} during create for unit: {} with message: {}".format(result, unit.get_id(), msg)
            self.fail_no_update(unit=unit, message=notice)

        try:
            self.get_substrate_database().update_unit(u=unit)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process create complete")

    def process_delete_complete(self, *, unit: ConfigToken, properties: dict):
        self.logger.debug("Delete")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise PluginException(Constants.INVALID_ACTOR_STATE)

        sequence = HandlerProcessor.get_action_sequence_number(properties=properties)
        notice = None
        # TODO synchronized on token
        if sequence != unit.get_sequence():
            self.logger.warning("(delete complete) sequences mismatch: incoming ({}) local: ({}). "
                                "Ignoring event.".format(sequence, unit.get_sequence()))
            return
        else:
            self.logger.debug("(delete complete) incoming ({}) local: ({})".format(sequence, unit.get_sequence()))

        result = HandlerProcessor.get_result_code(properties=properties)
        msg = HandlerProcessor.get_exception_message(properties=properties)
        if msg is None:
            msg = HandlerProcessor.get_result_code_message(properties=properties)

        if result == 0:
            self.logger.debug("delete code 0 (success)")
            self.merge_unit_properties(unit=unit, properties=properties)
            unit.close()

        elif result == -1:
            self.logger.debug("delete code -1 with message: {}".format(msg))
            notice = "Exception during delete for unit: {} {}".format(unit.get_id(), msg)
            self.fail_no_update(unit=unit, message=notice)

        else:
            self.logger.debug("delete code {} with message: {}".format(result, msg))
            notice = "Error code= {} during delete for unit: {} with message: {}".format(result, unit.get_id(), msg)
            self.fail_no_update(unit=unit, message=notice)

        try:
            self.get_substrate_database().update_unit(u=unit)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process delete complete")

    def process_modify_complete(self, *, unit: ConfigToken, properties: dict):
        self.logger.debug("Modify")
        self.logger.debug(properties)

        if self.actor.is_stopped():
            raise PluginException(Constants.INVALID_ACTOR_STATE)

        sequence = HandlerProcessor.get_action_sequence_number(properties=properties)
        notice = None
        if sequence != unit.get_sequence():
            self.logger.warning("(modify complete) sequences mismatch: incoming ({}) local: ({}). "
                                "Ignoring event.".format(sequence, unit.get_sequence()))
            return
        else:
            self.logger.debug("(modify complete) incoming ({}) local: ({})".format(sequence, unit.get_sequence()))

        result = HandlerProcessor.get_result_code(properties=properties)
        msg = HandlerProcessor.get_exception_message(properties=properties)
        if msg is None:
            msg = HandlerProcessor.get_result_code_message(properties=properties)

        if result == 0:
            self.logger.debug("modify code 0 (success)")
            self.merge_unit_properties(unit=unit, properties=properties)
            unit.complete_modify()

        elif result == -1:
            self.logger.debug("modify code -1 with message: {}".format(msg))
            notice = "Exception during modify for unit: {} {}".format(unit.get_id(), msg)
            self.fail_modify_no_update(unit=unit, message=notice)

        else:
            self.logger.debug("modify code {} with message: {}".format(result, msg))
            notice = "Error code= {} during modify for unit: {} with message: {}".format(result, unit.get_id(), msg)
            self.fail_modify_no_update(unit=unit, message=notice)

        try:
            self.get_substrate_database().update_unit(u=unit)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.debug("process modify complete")

    def get_substrate_database(self) -> ABCSubstrateDatabase:
        return self.db

    def set_database(self, *, db: ABCDatabase):
        if db is not None and not isinstance(db, ABCSubstrateDatabase):
            raise PluginException("db must implement ISubstrateDatabase")

        super().set_database(db=db)

    def update_props(self, *, reservation: ABCReservationMixin, unit: Unit):
        try:
            self.get_substrate_database().update_unit(u=unit)
        except Exception as e:
            self.fail_and_update(unit=unit, message="update properties error", e=e)
