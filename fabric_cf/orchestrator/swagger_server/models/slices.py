# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server.models.slice import Slice  # noqa: F401,E501
from fabric_cf.orchestrator.swagger_server.models.status200_ok_paginated import Status200OkPaginated  # noqa: F401,E501
from fabric_cf.orchestrator.swagger_server.models.status200_ok_paginated_links import Status200OkPaginatedLinks  # noqa: F401,E501
from fabric_cf.orchestrator.swagger_server import util


class Slices(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, data: List[Slice]=None, limit: int=None, links: Status200OkPaginatedLinks=None, offset: int=None, size: int=None, status: int=200, total: int=None, type: str=None):  # noqa: E501
        """Slices - a model defined in Swagger

        :param data: The data of this Slices.  # noqa: E501
        :type data: List[Slice]
        :param limit: The limit of this Slices.  # noqa: E501
        :type limit: int
        :param links: The links of this Slices.  # noqa: E501
        :type links: Status200OkPaginatedLinks
        :param offset: The offset of this Slices.  # noqa: E501
        :type offset: int
        :param size: The size of this Slices.  # noqa: E501
        :type size: int
        :param status: The status of this Slices.  # noqa: E501
        :type status: int
        :param total: The total of this Slices.  # noqa: E501
        :type total: int
        :param type: The type of this Slices.  # noqa: E501
        :type type: str
        """
        self.swagger_types = {
            'data': List[Slice],
            'limit': int,
            'links': Status200OkPaginatedLinks,
            'offset': int,
            'size': int,
            'status': int,
            'total': int,
            'type': str
        }

        self.attribute_map = {
            'data': 'data',
            'limit': 'limit',
            'links': 'links',
            'offset': 'offset',
            'size': 'size',
            'status': 'status',
            'total': 'total',
            'type': 'type'
        }
        self._data = data
        self._limit = limit
        self._links = links
        self._offset = offset
        self._size = size
        self._status = status
        self._total = total
        self._type = type

    @classmethod
    def from_dict(cls, dikt) -> 'Slices':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The slices of this Slices.  # noqa: E501
        :rtype: Slices
        """
        return util.deserialize_model(dikt, cls)

    @property
    def data(self) -> List[Slice]:
        """Gets the data of this Slices.


        :return: The data of this Slices.
        :rtype: List[Slice]
        """
        return self._data

    @data.setter
    def data(self, data: List[Slice]):
        """Sets the data of this Slices.


        :param data: The data of this Slices.
        :type data: List[Slice]
        """

        self._data = data

    @property
    def limit(self) -> int:
        """Gets the limit of this Slices.


        :return: The limit of this Slices.
        :rtype: int
        """
        return self._limit

    @limit.setter
    def limit(self, limit: int):
        """Sets the limit of this Slices.


        :param limit: The limit of this Slices.
        :type limit: int
        """

        self._limit = limit

    @property
    def links(self) -> Status200OkPaginatedLinks:
        """Gets the links of this Slices.


        :return: The links of this Slices.
        :rtype: Status200OkPaginatedLinks
        """
        return self._links

    @links.setter
    def links(self, links: Status200OkPaginatedLinks):
        """Sets the links of this Slices.


        :param links: The links of this Slices.
        :type links: Status200OkPaginatedLinks
        """

        self._links = links

    @property
    def offset(self) -> int:
        """Gets the offset of this Slices.


        :return: The offset of this Slices.
        :rtype: int
        """
        return self._offset

    @offset.setter
    def offset(self, offset: int):
        """Sets the offset of this Slices.


        :param offset: The offset of this Slices.
        :type offset: int
        """

        self._offset = offset

    @property
    def size(self) -> int:
        """Gets the size of this Slices.


        :return: The size of this Slices.
        :rtype: int
        """
        return self._size

    @size.setter
    def size(self, size: int):
        """Sets the size of this Slices.


        :param size: The size of this Slices.
        :type size: int
        """

        self._size = size

    @property
    def status(self) -> int:
        """Gets the status of this Slices.


        :return: The status of this Slices.
        :rtype: int
        """
        return self._status

    @status.setter
    def status(self, status: int):
        """Sets the status of this Slices.


        :param status: The status of this Slices.
        :type status: int
        """

        self._status = status

    @property
    def total(self) -> int:
        """Gets the total of this Slices.


        :return: The total of this Slices.
        :rtype: int
        """
        return self._total

    @total.setter
    def total(self, total: int):
        """Sets the total of this Slices.


        :param total: The total of this Slices.
        :type total: int
        """

        self._total = total

    @property
    def type(self) -> str:
        """Gets the type of this Slices.


        :return: The type of this Slices.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this Slices.


        :param type: The type of this Slices.
        :type type: str
        """

        self._type = type