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
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

case class LinearGnocchiModel(variantModels: Dataset[LinearVariantModel],
                              phenotypeNames: String,
                              covariatesNames: List[String],
                              numSamples: Int,
                              allelicAssumption: String = "ADDITIVE")
    extends GnocchiModel[LinearVariantModel, LinearGnocchiModel] {

  import variantModels.sqlContext.implicits._

  def mergeGnocchiModel(otherModel: LinearGnocchiModel): LinearGnocchiModel = {

    //    require(otherModel.metaData.modelType == metaData.modelType,
    //      "Models being merged are not the same type. Type equality is required to merge two models correctly.")

    val mergedVMs = mergeVariantModels(otherModel.variantModels)

    // ToDo: Ensure that the same phenotypes and covariates are being used
    LinearGnocchiModel(mergedVMs,
      phenotypeNames,
      covariatesNames,
      otherModel.numSamples + numSamples,
      allelicAssumption)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID"))
      .map(x => x._1.mergeWith(x._2))
  }

  //  /**
  //   * this function can be used in two ways
  //   *  1. pass in just genotypes and phenotypes -> this assumes that you are just applying this model
  //   *    to the parameter dataset and nothing else. It will only calculate the significance over that
  //   *    particular dataset
  //   *  2. set the additionalNumSamples and the partialSSResiduals to the number of samples and sum of
  //   *    squared residuals from an existing association which will build an aggregated association over
  //   *    both the existing association and the new dataset being passed in. This is used for incremental
  //   *    model building.
  //   *
  //   * @param genotypes raw genotypes to calculate the significance of
  //   * @param phenotypes raw phenotypes to calculate the significance of
  //   * @param additionalNumSamples
  //   * @param partialSSResiduals
  //   * @return
  //   */
  //  def calculateSignificance(genotypes: Dataset[CalledVariant],
  //                            phenotypes: Map[String, Phenotype],
  //                            additionalNumSamples: Option[Int] = None,
  //                            partialSSResiduals: Option[Double] = None): Dataset[LinearAssociation] = {
  //
  //    require(additionalNumSamples.isDefined == partialSSResiduals.isDefined,
  //      "You need to either define both partialSSResiduals and additionalNumSamples, or neither.")
  //
  //    // todo: broadcast phenotypes
  //
  //    variantModels.joinWith(genotypes, variantModels("uniqueID") === genotypes("uniqueID"))
  //      .map(i => {
  //
  //      })
  //  }
}
