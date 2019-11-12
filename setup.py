#!/usr/bin/env python

import os
import sys

if sys.version_info < (3, 0):
    raise ImportError("please use python 3.x")

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
version_contents = {}

with open(os.path.join(here, "firefly", "version.py"), encoding="utf-8") as f:
    exec(f.read(), version_contents)

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='firefly',
    version=version_contents["__version__"],
    description='Python clients for Firefly API',
    long_description=long_description,
    author='Firefly.ai',
    packages=find_packages(exclude=["tests", "tests.*"]),

    platforms='any',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console'
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',

    ],
    tests_require=[
        'pytest==3.1.0',
    ],
    install_requires=[
        "requests==2.20.0",
        "Flask==1.0.2",
        "flask-log-request-id==0.10.0"
    ],
)
