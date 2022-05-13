# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server import util


class Status200OkPaginatedLinks(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, first: str=None, last: str=None, next: str=None, prev: str=None):  # noqa: E501
        """Status200OkPaginatedLinks - a model defined in Swagger

        :param first: The first of this Status200OkPaginatedLinks.  # noqa: E501
        :type first: str
        :param last: The last of this Status200OkPaginatedLinks.  # noqa: E501
        :type last: str
        :param next: The next of this Status200OkPaginatedLinks.  # noqa: E501
        :type next: str
        :param prev: The prev of this Status200OkPaginatedLinks.  # noqa: E501
        :type prev: str
        """
        self.swagger_types = {
            'first': str,
            'last': str,
            'next': str,
            'prev': str
        }

        self.attribute_map = {
            'first': 'first',
            'last': 'last',
            'next': 'next',
            'prev': 'prev'
        }
        self._first = first
        self._last = last
        self._next = next
        self._prev = prev

    @classmethod
    def from_dict(cls, dikt) -> 'Status200OkPaginatedLinks':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The status_200_ok_paginated_links of this Status200OkPaginatedLinks.  # noqa: E501
        :rtype: Status200OkPaginatedLinks
        """
        return util.deserialize_model(dikt, cls)

    @property
    def first(self) -> str:
        """Gets the first of this Status200OkPaginatedLinks.


        :return: The first of this Status200OkPaginatedLinks.
        :rtype: str
        """
        return self._first

    @first.setter
    def first(self, first: str):
        """Sets the first of this Status200OkPaginatedLinks.


        :param first: The first of this Status200OkPaginatedLinks.
        :type first: str
        """

        self._first = first

    @property
    def last(self) -> str:
        """Gets the last of this Status200OkPaginatedLinks.


        :return: The last of this Status200OkPaginatedLinks.
        :rtype: str
        """
        return self._last

    @last.setter
    def last(self, last: str):
        """Sets the last of this Status200OkPaginatedLinks.


        :param last: The last of this Status200OkPaginatedLinks.
        :type last: str
        """

        self._last = last

    @property
    def next(self) -> str:
        """Gets the next of this Status200OkPaginatedLinks.


        :return: The next of this Status200OkPaginatedLinks.
        :rtype: str
        """
        return self._next

    @next.setter
    def next(self, next: str):
        """Sets the next of this Status200OkPaginatedLinks.


        :param next: The next of this Status200OkPaginatedLinks.
        :type next: str
        """

        self._next = next

    @property
    def prev(self) -> str:
        """Gets the prev of this Status200OkPaginatedLinks.


        :return: The prev of this Status200OkPaginatedLinks.
        :rtype: str
        """
        return self._prev

    @prev.setter
    def prev(self, prev: str):
        """Sets the prev of this Status200OkPaginatedLinks.


        :param prev: The prev of this Status200OkPaginatedLinks.
        :type prev: str
        """

        self._prev = prev
