/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel

case class LinearGnocchiModel(variantModels: Dataset[LinearVariantModel],
                              phenotypeNames: String,
                              covariatesNames: List[String],
                              sampleUIDs: Set[String],
                              numSamples: Int,
                              allelicAssumption: String)
    extends GnocchiModel[LinearVariantModel, LinearGnocchiModel] {

  import variantModels.sqlContext.implicits._

  def mergeGnocchiModel(otherModel: LinearGnocchiModel): LinearGnocchiModel = {

    require((otherModel.sampleUIDs & sampleUIDs).isEmpty,
      "There are overlapping samples in the datasets used to build the two models.")
    require(allelicAssumption == otherModel.allelicAssumption,
      "The two models are of different allelic assumptions. (additive / dominant / recessive)")

    val mergedVMs = mergeVariantModels(otherModel.variantModels)
    // ToDo: Ensure that the same phenotypes and covariates are being used
    LinearGnocchiModel(mergedVMs,
      phenotypeNames,
      covariatesNames,
      otherModel.sampleUIDs.toSet | sampleUIDs.toSet,
      otherModel.numSamples + numSamples,
      allelicAssumption)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID"))
      .map(x => x._1.mergeWith(x._2))
  }
}
