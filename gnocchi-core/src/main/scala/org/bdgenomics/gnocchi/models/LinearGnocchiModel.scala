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

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.models.variant.{ LinearVariantModel, QualityControlVariantModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

case class LinearGnocchiModel(metaData: GnocchiModelMetaData,
                              variantModels: Dataset[LinearVariantModel])
    extends GnocchiModel[LinearVariantModel, LinearGnocchiModel] {

  import variantModels.sqlContext.implicits._

  def mergeGnocchiModel(otherModel: GnocchiModel[LinearVariantModel, LinearGnocchiModel]): LinearGnocchiModel = {

    require(otherModel.metaData.modelType == metaData.modelType,
      "Models being merged are not the same type. Type equality is required to merge two models correctly.")

    val mergedVMs = mergeVariantModels(otherModel.variantModels)

    // ToDo: 1. [DONE] make sure models are of same type 2. [DONE] find intersection of QCVariants and use those as the gnocchiModel
    // ToDo: QCVariants 3. Make sure the phenotype of the models are the same 4. Make sure the covariates of the model
    // ToDo: are the same (currently broken because covariates stored in [[Phenotype]] object are the values not names)
    val updatedMetaData = updateMetaData(otherModel.metaData.numSamples)

    LinearGnocchiModel(updatedMetaData, mergedVMs)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  }
}
