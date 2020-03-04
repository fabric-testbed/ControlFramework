# coding: utf-8

from setuptools import setup, find_packages

from actor import ConfDir, ConfFile, LogDir, LogFile

NAME = "actor"
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
    description="Base Fabric Actor API",
    author_email="kthare10@renci.org",
    url="",
    keywords=["Swagger", "Base Fabric Actor API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['actor/swagger_server/swagger/swagger.yaml']},
    include_package_data=True,
    data_files = [(ConfDir, [ConfFile]), (LogDir, [LogFile])],
    entry_points={
        'console_scripts': ['actor.swagger_server=actor.swagger_server.__main__:main']},
    long_description="""\
    This is a base fabric API
    """
)
