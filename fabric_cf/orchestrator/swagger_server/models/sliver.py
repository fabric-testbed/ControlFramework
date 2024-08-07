# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server import util


class Sliver(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, notice: str=None, sliver_type: str=None, sliver: object=None, lease_start_time: str=None, lease_end_time: str=None, state: str=None, pending_state: str=None, join_state: str=None, graph_node_id: str=None, slice_id: str=None, sliver_id: str=None, owner_user_id: str=None, owner_email: str=None):  # noqa: E501
        """Sliver - a model defined in Swagger

        :param notice: The notice of this Sliver.  # noqa: E501
        :type notice: str
        :param sliver_type: The sliver_type of this Sliver.  # noqa: E501
        :type sliver_type: str
        :param sliver: The sliver of this Sliver.  # noqa: E501
        :type sliver: object
        :param lease_start_time: The lease_start_time of this Sliver.  # noqa: E501
        :type lease_start_time: str
        :param lease_end_time: The lease_end_time of this Sliver.  # noqa: E501
        :type lease_end_time: str
        :param state: The state of this Sliver.  # noqa: E501
        :type state: str
        :param pending_state: The pending_state of this Sliver.  # noqa: E501
        :type pending_state: str
        :param join_state: The join_state of this Sliver.  # noqa: E501
        :type join_state: str
        :param graph_node_id: The graph_node_id of this Sliver.  # noqa: E501
        :type graph_node_id: str
        :param slice_id: The slice_id of this Sliver.  # noqa: E501
        :type slice_id: str
        :param sliver_id: The sliver_id of this Sliver.  # noqa: E501
        :type sliver_id: str
        :param owner_user_id: The owner_user_id of this Sliver.  # noqa: E501
        :type owner_user_id: str
        :param owner_email: The owner_email of this Sliver.  # noqa: E501
        :type owner_email: str
        """
        self.swagger_types = {
            'notice': str,
            'sliver_type': str,
            'sliver': object,
            'lease_start_time': str,
            'lease_end_time': str,
            'state': str,
            'pending_state': str,
            'join_state': str,
            'graph_node_id': str,
            'slice_id': str,
            'sliver_id': str,
            'owner_user_id': str,
            'owner_email': str
        }

        self.attribute_map = {
            'notice': 'notice',
            'sliver_type': 'sliver_type',
            'sliver': 'sliver',
            'lease_start_time': 'lease_start_time',
            'lease_end_time': 'lease_end_time',
            'state': 'state',
            'pending_state': 'pending_state',
            'join_state': 'join_state',
            'graph_node_id': 'graph_node_id',
            'slice_id': 'slice_id',
            'sliver_id': 'sliver_id',
            'owner_user_id': 'owner_user_id',
            'owner_email': 'owner_email'
        }
        self._notice = notice
        self._sliver_type = sliver_type
        self._sliver = sliver
        self._lease_start_time = lease_start_time
        self._lease_end_time = lease_end_time
        self._state = state
        self._pending_state = pending_state
        self._join_state = join_state
        self._graph_node_id = graph_node_id
        self._slice_id = slice_id
        self._sliver_id = sliver_id
        self._owner_user_id = owner_user_id
        self._owner_email = owner_email

    @classmethod
    def from_dict(cls, dikt) -> 'Sliver':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The sliver of this Sliver.  # noqa: E501
        :rtype: Sliver
        """
        return util.deserialize_model(dikt, cls)

    @property
    def notice(self) -> str:
        """Gets the notice of this Sliver.


        :return: The notice of this Sliver.
        :rtype: str
        """
        return self._notice

    @notice.setter
    def notice(self, notice: str):
        """Sets the notice of this Sliver.


        :param notice: The notice of this Sliver.
        :type notice: str
        """

        self._notice = notice

    @property
    def sliver_type(self) -> str:
        """Gets the sliver_type of this Sliver.


        :return: The sliver_type of this Sliver.
        :rtype: str
        """
        return self._sliver_type

    @sliver_type.setter
    def sliver_type(self, sliver_type: str):
        """Sets the sliver_type of this Sliver.


        :param sliver_type: The sliver_type of this Sliver.
        :type sliver_type: str
        """

        self._sliver_type = sliver_type

    @property
    def sliver(self) -> object:
        """Gets the sliver of this Sliver.


        :return: The sliver of this Sliver.
        :rtype: object
        """
        return self._sliver

    @sliver.setter
    def sliver(self, sliver: object):
        """Sets the sliver of this Sliver.


        :param sliver: The sliver of this Sliver.
        :type sliver: object
        """

        self._sliver = sliver

    @property
    def lease_start_time(self) -> str:
        """Gets the lease_start_time of this Sliver.


        :return: The lease_start_time of this Sliver.
        :rtype: str
        """
        return self._lease_start_time

    @lease_start_time.setter
    def lease_start_time(self, lease_start_time: str):
        """Sets the lease_start_time of this Sliver.


        :param lease_start_time: The lease_start_time of this Sliver.
        :type lease_start_time: str
        """

        self._lease_start_time = lease_start_time

    @property
    def lease_end_time(self) -> str:
        """Gets the lease_end_time of this Sliver.


        :return: The lease_end_time of this Sliver.
        :rtype: str
        """
        return self._lease_end_time

    @lease_end_time.setter
    def lease_end_time(self, lease_end_time: str):
        """Sets the lease_end_time of this Sliver.


        :param lease_end_time: The lease_end_time of this Sliver.
        :type lease_end_time: str
        """

        self._lease_end_time = lease_end_time

    @property
    def state(self) -> str:
        """Gets the state of this Sliver.


        :return: The state of this Sliver.
        :rtype: str
        """
        return self._state

    @state.setter
    def state(self, state: str):
        """Sets the state of this Sliver.


        :param state: The state of this Sliver.
        :type state: str
        """

        self._state = state

    @property
    def pending_state(self) -> str:
        """Gets the pending_state of this Sliver.


        :return: The pending_state of this Sliver.
        :rtype: str
        """
        return self._pending_state

    @pending_state.setter
    def pending_state(self, pending_state: str):
        """Sets the pending_state of this Sliver.


        :param pending_state: The pending_state of this Sliver.
        :type pending_state: str
        """

        self._pending_state = pending_state

    @property
    def join_state(self) -> str:
        """Gets the join_state of this Sliver.


        :return: The join_state of this Sliver.
        :rtype: str
        """
        return self._join_state

    @join_state.setter
    def join_state(self, join_state: str):
        """Sets the join_state of this Sliver.


        :param join_state: The join_state of this Sliver.
        :type join_state: str
        """

        self._join_state = join_state

    @property
    def graph_node_id(self) -> str:
        """Gets the graph_node_id of this Sliver.


        :return: The graph_node_id of this Sliver.
        :rtype: str
        """
        return self._graph_node_id

    @graph_node_id.setter
    def graph_node_id(self, graph_node_id: str):
        """Sets the graph_node_id of this Sliver.


        :param graph_node_id: The graph_node_id of this Sliver.
        :type graph_node_id: str
        """
        if graph_node_id is None:
            raise ValueError("Invalid value for `graph_node_id`, must not be `None`")  # noqa: E501

        self._graph_node_id = graph_node_id

    @property
    def slice_id(self) -> str:
        """Gets the slice_id of this Sliver.


        :return: The slice_id of this Sliver.
        :rtype: str
        """
        return self._slice_id

    @slice_id.setter
    def slice_id(self, slice_id: str):
        """Sets the slice_id of this Sliver.


        :param slice_id: The slice_id of this Sliver.
        :type slice_id: str
        """
        if slice_id is None:
            raise ValueError("Invalid value for `slice_id`, must not be `None`")  # noqa: E501

        self._slice_id = slice_id

    @property
    def sliver_id(self) -> str:
        """Gets the sliver_id of this Sliver.


        :return: The sliver_id of this Sliver.
        :rtype: str
        """
        return self._sliver_id

    @sliver_id.setter
    def sliver_id(self, sliver_id: str):
        """Sets the sliver_id of this Sliver.


        :param sliver_id: The sliver_id of this Sliver.
        :type sliver_id: str
        """
        if sliver_id is None:
            raise ValueError("Invalid value for `sliver_id`, must not be `None`")  # noqa: E501

        self._sliver_id = sliver_id

    @property
    def owner_user_id(self) -> str:
        """Gets the owner_user_id of this Sliver.


        :return: The owner_user_id of this Sliver.
        :rtype: str
        """
        return self._owner_user_id

    @owner_user_id.setter
    def owner_user_id(self, owner_user_id: str):
        """Sets the owner_user_id of this Sliver.


        :param owner_user_id: The owner_user_id of this Sliver.
        :type owner_user_id: str
        """

        self._owner_user_id = owner_user_id

    @property
    def owner_email(self) -> str:
        """Gets the owner_email of this Sliver.


        :return: The owner_email of this Sliver.
        :rtype: str
        """
        return self._owner_email

    @owner_email.setter
    def owner_email(self, owner_email: str):
        """Sets the owner_email of this Sliver.


        :param owner_email: The owner_email of this Sliver.
        :type owner_email: str
        """

        self._owner_email = owner_email
