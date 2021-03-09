# coding: utf-8

from setuptools import setup, find_packages

from fabric_cf import actor

NAME = "fabric-cf"
VERSION = "0.13"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = fh.read()

setup(
    name=NAME,
    version=VERSION,
    description="Fabric Control Framework",
    author="Komal Thareja",
    author_email="kthare10@renci.org",
    url="https://github.com/fabric-testbed/ControlFramework",
    keywords=["Swagger", "Fabric Control Framework"],
    install_requires=requirements,
    setup_requires=requirements,
    packages=find_packages(),
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)
