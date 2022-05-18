# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server import util


class Status200OkSingle(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, size: int=1, status: int=200, type: str=None):  # noqa: E501
        """Status200OkSingle - a model defined in Swagger

        :param size: The size of this Status200OkSingle.  # noqa: E501
        :type size: int
        :param status: The status of this Status200OkSingle.  # noqa: E501
        :type status: int
        :param type: The type of this Status200OkSingle.  # noqa: E501
        :type type: str
        """
        self.swagger_types = {
            'size': int,
            'status': int,
            'type': str
        }

        self.attribute_map = {
            'size': 'size',
            'status': 'status',
            'type': 'type'
        }
        self._size = size
        self._status = status
        self._type = type

    @classmethod
    def from_dict(cls, dikt) -> 'Status200OkSingle':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The status_200_ok_single of this Status200OkSingle.  # noqa: E501
        :rtype: Status200OkSingle
        """
        return util.deserialize_model(dikt, cls)

    @property
    def size(self) -> int:
        """Gets the size of this Status200OkSingle.


        :return: The size of this Status200OkSingle.
        :rtype: int
        """
        return self._size

    @size.setter
    def size(self, size: int):
        """Sets the size of this Status200OkSingle.


        :param size: The size of this Status200OkSingle.
        :type size: int
        """

        self._size = size

    @property
    def status(self) -> int:
        """Gets the status of this Status200OkSingle.


        :return: The status of this Status200OkSingle.
        :rtype: int
        """
        return self._status

    @status.setter
    def status(self, status: int):
        """Sets the status of this Status200OkSingle.


        :param status: The status of this Status200OkSingle.
        :type status: int
        """

        self._status = status

    @property
    def type(self) -> str:
        """Gets the type of this Status200OkSingle.


        :return: The type of this Status200OkSingle.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this Status200OkSingle.


        :param type: The type of this Status200OkSingle.
        :type type: str
        """

        self._type = type
