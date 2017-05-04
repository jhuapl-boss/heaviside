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

__version__ = '1.0'

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

here = os.path.abspath(os.path.dirname(__file__))
def read(filename):
    with open(os.path.join(here, filename), 'r') as fh:
        return fh.read()

def test_suite():
    import unittest
    loader = unittest.TestLoader()
    suite = loader.discover('heaviside', pattern='test_*.py')
    return suite

setup(
    name='heaviside',
    version=__version__,
    packages=['heaviside'],
    url='https://github.com/jhuapl-boss/heaviside',
    license="Apache Software License 2.0",
    author='Derek Pryor',
    author_email='Derek.Pryor@jhuapl.edu',
    description='Python library and DSL for working with AWS StepFunctions',
    long_description=read('README.md'),
    install_requires=read('requirements.txt').split('\n'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 2.7',
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
    ],
    test_suite='setup.test_suite'
)
