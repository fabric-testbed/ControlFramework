# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from fabric_cf.orchestrator.swagger_server.models.base_model_ import Model
from fabric_cf.orchestrator.swagger_server import util


class SlicesPost(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, graph_model: str=None, ssh_keys: List[str]=None):  # noqa: E501
        """SlicesPost - a model defined in Swagger

        :param graph_model: The graph_model of this SlicesPost.  # noqa: E501
        :type graph_model: str
        :param ssh_keys: The ssh_keys of this SlicesPost.  # noqa: E501
        :type ssh_keys: List[str]
        """
        self.swagger_types = {
            'graph_model': str,
            'ssh_keys': List[str]
        }

        self.attribute_map = {
            'graph_model': 'graph_model',
            'ssh_keys': 'ssh_keys'
        }
        self._graph_model = graph_model
        self._ssh_keys = ssh_keys

    @classmethod
    def from_dict(cls, dikt) -> 'SlicesPost':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The slices_post of this SlicesPost.  # noqa: E501
        :rtype: SlicesPost
        """
        return util.deserialize_model(dikt, cls)

    @property
    def graph_model(self) -> str:
        """Gets the graph_model of this SlicesPost.


        :return: The graph_model of this SlicesPost.
        :rtype: str
        """
        return self._graph_model

    @graph_model.setter
    def graph_model(self, graph_model: str):
        """Sets the graph_model of this SlicesPost.


        :param graph_model: The graph_model of this SlicesPost.
        :type graph_model: str
        """
        if graph_model is None:
            raise ValueError("Invalid value for `graph_model`, must not be `None`")  # noqa: E501

        self._graph_model = graph_model

    @property
    def ssh_keys(self) -> List[str]:
        """Gets the ssh_keys of this SlicesPost.


        :return: The ssh_keys of this SlicesPost.
        :rtype: List[str]
        """
        return self._ssh_keys

    @ssh_keys.setter
    def ssh_keys(self, ssh_keys: List[str]):
        """Sets the ssh_keys of this SlicesPost.


        :param ssh_keys: The ssh_keys of this SlicesPost.
        :type ssh_keys: List[str]
        """
        if ssh_keys is None:
            raise ValueError("Invalid value for `ssh_keys`, must not be `None`")  # noqa: E501

        self._ssh_keys = ssh_keys
