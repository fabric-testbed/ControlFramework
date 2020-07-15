# coding: utf-8

from setuptools import setup, find_packages

from fabric import actor

NAME = "fabric-actor"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = [
            'yapsy == 1.12.2',
            'python_dateutil == 2.6.0',
            'setuptools >= 21.0.0',
            'requests',
            'PyJWT >=1.7.1',
            'cryptography',
            'psycopg2-binary',
            'sqlalchemy',
            'pyyaml',
            'fabric-message-bus'
            ]

setup(
    name=NAME,
    version=VERSION,
    description="Fabric Control Framework",
    author="Komal Thareja",
    author_email="kthare10@renci.org",
    url="https://github.com/fabric-testbed/ActorBase",
    keywords=["Swagger", "Fabric Control Framework"],
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True,
    data_files=[(actor.ConfDir, [actor.ConfFile]), (actor.LogDir, [])],
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    classifiers=[
                  "Programming Language :: Python :: 3",
                  "License :: OSI Approved :: MIT License",
                  "Operating System :: OS Independent",
              ],
    python_requires='>=3.7'
)
