# coding: utf-8

from setuptools import setup, find_packages
import actor
import controller

NAME = "controller"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = [
            'yapsy == 1.12.2',
            'connexion == 2.6.0',
            'python_dateutil == 2.6.0',
            'setuptools >= 21.0.0',
            'requests',
            'PyJWT >=1.7.1',
            'connexion[swagger-ui]',
            'cryptography'
            ]

setup(
    name=NAME,
    version=VERSION,
    description="Fabric Control Framework",
    author_email="kthare10@renci.org",
    url="",
    keywords=["Swagger", "Fabric Control Framework"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['controller/swagger_server/swagger/swagger.yaml']},
    include_package_data=True,
    data_files = [(actor.ConfDir, [actor.ConfFile]), (actor.LogDir, [actor.LogFile]),
                  (controller.ConfDir, [controller.ConfFile]), (controller.LogDir, [controller.LogFile])],
    entry_points={
        'console_scripts': ['controller.swagger_server=controller.swagger_server.__main__:main']},
    long_description="""\
    Fabric Control Framework
    """
)
