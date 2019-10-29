#!/usr/bin/env python

import io
import os
import sys

if sys.version_info < (3, 0):
    raise ImportError("please use python 3.x")

from setuptools import setup, find_packages


def read(fname):
    return io.open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


exec(open('firefly/version.py').read())

setup(
    name='firefly',
    version=__version__,
    description='Firefly python sdk',
    long_description=read('README.md'),

    packages=find_packages(),

    platforms='any',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console'
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
