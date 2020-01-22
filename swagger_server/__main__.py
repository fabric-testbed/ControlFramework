#!/usr/bin/env python3

import connexion
from yapsy.PluginManager import PluginManagerSingleton

from swagger_server import encoder


def main():
    # Load the plugins from the plugin directory.
    manager = PluginManagerSingleton().get()
    manager.setPluginPlaces(["plugins"])
    manager.collectPlugins()

    # Loop round the plugins and print their names.
    for plugin in manager.getAllPlugins():
        plugin.plugin_object.print_name()
        plugin.plugin_object.configure(None)

    app = connexion.App(__name__, specification_dir='./swagger/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'Base Fabric Actor API'}, pythonic_params=True)
    app.run(port=8080)


if __name__ == '__main__':
    main()
