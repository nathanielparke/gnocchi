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

import java.io.{ File, PrintWriter }

import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, VariantModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.apache.spark.sql.Dataset
import java.io.FileOutputStream
import java.io.ObjectOutputStream

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.apache.hadoop.fs.Path
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.sql.functions.col

import scala.collection.immutable.Map

case class GnocchiModelMetaData(modelType: String,
                                phenotype: String,
                                covariates: String,
                                numSamples: Int,
                                haplotypeBlockErrorThreshold: Double = 0.1,
                                flaggedVariantModels: Option[List[String]] = None)

trait GnocchiModel[VM <: VariantModel[VM], GM <: GnocchiModel[VM, GM]] {

  val metaData: GnocchiModelMetaData

  val variantModels: Dataset[VM]

  //  def mergeGnocchiModel(otherModel: GnocchiModel[VM, GM]): GnocchiModel[VM, GM]

  def mergeVariantModels(newVariantModels: Dataset[VM]): Dataset[VM]

  /**
   * Returns new GnocchiModelMetaData object with all fields copied except
   * numSamples and flaggedVariantModels updated.
   *
   * @param numAdditionalSamples Number of samples in update data
   * @param newFlaggedVariantModels VariantModels flagged after update
   */
  def updateMetaData(numAdditionalSamples: Int,
                     newFlaggedVariantModels: Option[List[String]] = None): GnocchiModelMetaData = {
    val numSamples = this.metaData.numSamples + numAdditionalSamples

    GnocchiModelMetaData(
      this.metaData.modelType,
      this.metaData.phenotype,
      this.metaData.covariates,
      numSamples,
      this.metaData.haplotypeBlockErrorThreshold,
      if (newFlaggedVariantModels.isDefined) newFlaggedVariantModels else this.metaData.flaggedVariantModels)
  }

  //  def predict(genotypes: Dataset[CalledVariant],
  //              covariates: Map[String, List[Double]],
  //              numPredictors: Int = 100): Array[(String, Double)] = {
  //    // broadcast this
  //    val covarMat = covariates.map(f => {
  //      (f._1, DenseVector(f._2: _*).t)
  //    })
  //
  //    val covarVals = variantModels.sort(col("association.pValue").asc).repartition(40).limit(numPredictors).rdd.flatMap(f => {
  //      val weights = DenseVector(f.association.weights.drop(2): _*)
  //      covarMat.map(g => {
  //        ((f.uniqueID, g._1), (f.association.weights(1), g._2 * weights + f.association.weights(0)))
  //      })
  //    }).filter(f => !f._2._2.isNaN)
  //
  //    val genotypes_2 = genotypes.repartition(40).rdd.flatMap(f => f.samples.map(g => ((f.uniqueID, g.sampleID), g.alts.toDouble)))
  //
  //    val y_hat = genotypes_2.join(covarVals).map(f => {
  //      (f._1._2, f._2._1 * f._2._2._1 + f._2._2._2)
  //    }).mapValues(x => (x, 1))
  //
  //    y_hat.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).collect
  //  }
}
