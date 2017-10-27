#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from setuptools import find_packages, setup
from version import version as gnocchi_version

setup(
    name='bdgenomics.gnocchi',
    version=gnocchi_version,
    description='gnocchi: Genotype store and query engine',
    author='',
    author_email='',
    url="https://github.com/bdgenomics/gnocchi",
    install_requires=[],
    packages=find_packages(exclude=['*.test.*']))