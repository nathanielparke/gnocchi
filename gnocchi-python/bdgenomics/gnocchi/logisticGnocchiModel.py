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

from bdgenomics.gnocchi.primitives import CalledVariantDataset, PhenotypeMap, LogisticVariantModelDataset
from py4j.java_collections import ListConverter

class LogisticGnocchiModel(object):

    def __init__(self, ss,
                 genotypes,
                 phenotypes,
                 phenotypeNames,
                 QCVariantIDs,
                 QCVariantSamplingRate = 0.1,
                 allelicAssumption = "ADDITIVE",
                 validationStringency = "STRICT"):
        self._sc = ss.sparkContext
        self._jvm = self._sc._jvm
        session = self._jvm.org.bdgenomics.gnocchi.sql.GnocchiSession.GnocchiSessionFromSession(ss._jsparkSession)
        self.__jlgmf = self._jvm.org.bdgenomics.gnocchi.api.java.JavaLogisticGnocchiModelFactory
        self.__jlgmf.generate(session)

        self.__lgm = self.__jlgmf.apply(genotypes,
               phenotypes,
                ListConverter().convert(phenotypeNames, self._sc._gateway._gateway_client),
                ListConverter().convert(QCVariantIDs, self._sc._gateway._gateway_client),
               QCVariantSamplingRate,
               allelicAssumption,
               validationStringency)

        self.__jlgm = self._jvm.org.bdgenomics.gnocchi.api.java.JavaLogisticGnocchiModel(self.__lgm)

    def get(self):
        return self.__jlgm

    def mergeGnocchiModel(self, otherModel):
        return self.__jlgm.mergeGnocchiModel(otherModel.get())

    def mergeVariantModels(self, newVariantModels):
        dataset = self.__jlgm.mergeVariantModels(newVariantModels)
        return LogisticVariantModelDataset(dataset, self._sc)

    def mergeQCVariants(self, newQCVariantModels):
        dataset = self.__jlgm.mergeQCVariants(newQCVariantModels)
        return CalledVariantDataset(dataset, self._sc)

