# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server.models.status200_ok_no_content_data import Status200OkNoContentData  # noqa: F401,E501
from fabric_cf.orchestrator.swagger_server import util


class Status200OkNoContent(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, data: List[Status200OkNoContentData]=None, type: str='no_content', size: int=1, status: int=200):  # noqa: E501
        """Status200OkNoContent - a model defined in Swagger

        :param data: The data of this Status200OkNoContent.  # noqa: E501
        :type data: List[Status200OkNoContentData]
        :param type: The type of this Status200OkNoContent.  # noqa: E501
        :type type: str
        :param size: The size of this Status200OkNoContent.  # noqa: E501
        :type size: int
        :param status: The status of this Status200OkNoContent.  # noqa: E501
        :type status: int
        """
        self.swagger_types = {
            'data': List[Status200OkNoContentData],
            'type': str,
            'size': int,
            'status': int
        }

        self.attribute_map = {
            'data': 'data',
            'type': 'type',
            'size': 'size',
            'status': 'status'
        }
        self._data = data
        self._type = type
        self._size = size
        self._status = status

    @classmethod
    def from_dict(cls, dikt) -> 'Status200OkNoContent':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The status_200_ok_no_content of this Status200OkNoContent.  # noqa: E501
        :rtype: Status200OkNoContent
        """
        return util.deserialize_model(dikt, cls)

    @property
    def data(self) -> List[Status200OkNoContentData]:
        """Gets the data of this Status200OkNoContent.


        :return: The data of this Status200OkNoContent.
        :rtype: List[Status200OkNoContentData]
        """
        return self._data

    @data.setter
    def data(self, data: List[Status200OkNoContentData]):
        """Sets the data of this Status200OkNoContent.


        :param data: The data of this Status200OkNoContent.
        :type data: List[Status200OkNoContentData]
        """

        self._data = data

    @property
    def type(self) -> str:
        """Gets the type of this Status200OkNoContent.


        :return: The type of this Status200OkNoContent.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this Status200OkNoContent.


        :param type: The type of this Status200OkNoContent.
        :type type: str
        """

        self._type = type

    @property
    def size(self) -> int:
        """Gets the size of this Status200OkNoContent.


        :return: The size of this Status200OkNoContent.
        :rtype: int
        """
        return self._size

    @size.setter
    def size(self, size: int):
        """Sets the size of this Status200OkNoContent.


        :param size: The size of this Status200OkNoContent.
        :type size: int
        """

        self._size = size

    @property
    def status(self) -> int:
        """Gets the status of this Status200OkNoContent.


        :return: The status of this Status200OkNoContent.
        :rtype: int
        """
        return self._status

    @status.setter
    def status(self, status: int):
        """Sets the status of this Status200OkNoContent.


        :param status: The status of this Status200OkNoContent.
        :type status: int
        """

        self._status = status
