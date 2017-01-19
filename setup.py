#!/usr/bin/env python
# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='heaviside',
    version='0.1',
    packages=['heaviside'],
    url='https://github.com/jhuapl-boss/heaviside',
    license="Apache Software License",
    author='Derek Pryor',
    author_email='Derek.Pryor@jhuapl.edu',
    description='Python library and DSL for working with AWS StepFunctions',
    install_requires=[
        'funcparserlib',
        'iso8601',
        'boto3>=1.4.3'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
    ],
    keywords=[
        'boss',
        'microns',
        'aws',
        'stepfunctions',
        'dsl',
    ],
    scripts=[
        'bin/heaviside'
    ]
)
