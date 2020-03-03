# coding: utf-8

import sys
from setuptools import setup, find_packages

from security import ConfDir, ConfFile, LogDir

NAME = "swagger_server"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["connexion"]

setup(
    name=NAME,
    version=VERSION,
    description="Base Fabric Actor API",
    author_email="kthare10@renci.org",
    url="",
    keywords=["Swagger", "Base Fabric Actor API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['swagger/swagger.yaml']},
    include_package_data=True,
    data_files = [(ConfDir, [ConfFile]), (LogDir, [])],
    entry_points={
        'console_scripts': ['swagger_server=swagger_server.__main__:main']},
    long_description="""\
    This is a base fabric API
    """
)
