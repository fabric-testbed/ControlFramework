import connexion
import six
from yapsy.PluginManager import PluginManagerSingleton

from swagger_server.models.actor_response import ActorResponse  # noqa: E501
from swagger_server import util


def create_post(body, resource_id):  # noqa: E501
    """creates resource(s)

    Request to create resource(s)  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: ActorResponse
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501

    # Invoke Controller Actor Plugin
    manager = PluginManagerSingleton.get()
    controller_plugin = manager.getPluginByName("controller")
    return controller_plugin.plugin_object.create_slice("defaultGuid", resource_id, body)


def delete_delete(body, resource_id):  # noqa: E501
    """delete resource(s)

    Request to delete resource(s)  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: ActorResponse
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def get_get(resource_id):  # noqa: E501
    """get manifest for the resource(s)

    Request to get manifest for resource(s)  # noqa: E501

    :param resource_id: 
    :type resource_id: str

    :rtype: ActorResponse
    """
    return 'do some magic!'


def modify_post(body, resource_id):  # noqa: E501
    """modify resource(s)

    Request to modify resource(s)  # noqa: E501

    :param body: 
    :type body: dict | bytes
    :param resource_id: 
    :type resource_id: str

    :rtype: ActorResponse
    """
    if connexion.request.is_json:
        body = str.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
