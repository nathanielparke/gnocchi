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
package org.bdgenomics.gnocchi.models.variant

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.apache.commons.math3.distribution.TDistribution
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

case class LinearVariantModel(uniqueID: String,
                              chromosome: Int,
                              position: Int,
                              referenceAllele: String,
                              alternateAllele: String,
                              numSamples: Int,
                              numPredictors: Int,
                              xTx: Array[Double],
                              xTy: Array[Double],
                              residualDegreesOfFreedom: Int,
                              weights: List[Double])
    extends VariantModel[LinearVariantModel] with LinearSiteRegression {
  def createAssociation(genotypes: CalledVariant,
                        phenotypes: Map[String, Phenotype]): LinearAssociation = {
    // toDo: fix allelic assumption
    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, "ADDITIVE")
    val breezeXtX = new DenseMatrix(numPredictors, numPredictors, xTx)
    val beta = new DenseVector(weights.toArray)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(x, y, beta, breezeXtX)

    LinearAssociation(uniqueID, chromosome, position, x.rows, pValue, genoSE, ssResiduals, t)
  }

  /**
   * Merges this [[LinearVariantModel]] with another that is passed in as an argument.
   *
   * @param variantModel The [[LinearVariantModel]] to merge with.
   * @return A new [[LinearVariantModel]] that is the result of merging the two models.
   */
  def mergeWith(variantModel: LinearVariantModel): LinearVariantModel = {
    val newXtXList = this.xTx.zip(variantModel.xTx).map { case (x, y) => x + y }
    val newXtX = new DenseMatrix(this.numPredictors, this.numPredictors, newXtXList)
    val newXtYList = this.xTy.zip(variantModel.xTy).map { case (x, y) => x + y }
    val newXtY = new DenseVector(newXtYList)
    val newNumSamples = variantModel.numSamples + this.numSamples
    val newResidualDegreesOfFreedom = newNumSamples - this.numPredictors
    val newWeights = newXtX \ newXtY

    LinearVariantModel(uniqueID, chromosome, position, referenceAllele, alternateAllele, newNumSamples, numPredictors, newXtXList.toArray, newXtYList.toArray, newResidualDegreesOfFreedom, newWeights.toArray.toList)
  }
}