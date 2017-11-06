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

from bdgenomics.gnocchi.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.test import SparkTestCase


class GnocchiContextTest(SparkTestCase):

    def test_load_genotypes(self):

        testFile = self.resourceFile("small1.vcf")
        gs = GnocchiSession(self.ss)

        genotypes = gs.loadGenotypes(testFile)

        self.assertEqual(genotypes._jvmDS.toJavaRDD().count(), 2)
