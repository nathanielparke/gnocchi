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
package net.fnothaft.gnocchi.algorithms.siteregression

import breeze.linalg._
import breeze.numerics.{ log10, _ }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel, LogisticVariantModel }
import net.fnothaft.gnocchi.primitives.association.LogisticAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.collection.immutable.Map

trait LogisticSiteRegression[VM <: LogisticVariantModel[VM]] extends SiteRegression[VM] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[VM]

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): LogisticAssociation = {

    val (data, labels) = prepareDesignMatrix(phenotypes, genotypes)

    val phenotypesLength = phenotypes.head._2.covariates.length + 1
    val numObservations = genotypes.samples.count(x => !x.value.contains("."))

    val iter = 0
    val maxIter = 1000
    val tolerance = 1e-6
    val initBeta = DenseVector.zeros[Double](data.cols)

    val (beta, hessian) = findBeta(data, labels, initBeta, iter = iter, maxIter = maxIter, tolerance = tolerance)

    /* Use Hessian and weights to calculate the Wald Statistic, or p-value */
    val fisherInfo = -hessian
    val fishInv = inv(fisherInfo)
    val standardErrors = sqrt(abs(diag(fishInv)))
    val genoStandardError = standardErrors(1)

    // calculate Wald statistic for each parameter in the regression model
    val zScores: DenseVector[Double] = DenseVector(beta) /:/ standardErrors
    val waldStats = zScores *:* zScores

    // calculate cumulative probs
    val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
    val probs = waldStats.map(zi => chiDist.cumulativeProbability(zi))

    val waldTests = 1d - probs

    LogisticAssociation(
      beta.toList,
      genoStandardError,
      waldTests(1),
      numObservations)
  }

  def findBeta(X: DenseMatrix[Double],
               Y: DenseVector[Double],
               beta: DenseVector[Double],
               iter: Int = 0,
               maxIter: Int = 1000,
               tolerance: Double = 1e-6): (Array[Double], DenseMatrix[Double]) = {

    val logitArray = X * beta

    val p = logitArray.map(x => Math.exp(-softmax(Array(0.0, -x))))

    val hessian = p.toArray.zipWithIndex.map { case (pi, i) => -X(i, ::).t * X(i, ::) * pi * (1.0 - pi) }.reduce(_ + _)
    val sampleScore = { X *:* tile(Y - p, 1, X.cols) }
    val score = sum(sampleScore(::, *)).t

    val update = -inv(hessian) * score
    val updatedBeta = beta + update

    if (updatedBeta.exists(_.isNaN)) logError("LOG_REG - Broke on iteration: " + iter)
    if (max(abs(update)) <= tolerance || iter + 1 == maxIter) return (updatedBeta.toArray, hessian)

    findBeta(X, Y, updatedBeta, iter = iter + 1, maxIter = maxIter, tolerance = tolerance)
  }

  def prepareDesignMatrix(phenotypes: Map[String, Phenotype],
                          genotypes: CalledVariant): (DenseMatrix[Double], DenseVector[Double]) = {

    val samplesGenotypes = genotypes.samples
      .filter { case genotypeState => !genotypeState.value.contains(".") }
      .map { case genotypeState => (genotypeState.sampleID, List(clipOrKeepState(genotypeState.toDouble))) }

    val cleanedSampleVector = samplesGenotypes
      .map { case (sampleID, genotype) => (sampleID, (genotype ++ phenotypes(sampleID).covariates).toList) }

    val XandY = cleanedSampleVector.map { case (sampleID, sampleVector) => (DenseVector(1.0 +: sampleVector.toArray), phenotypes(sampleID).phenotype) }
    val X = DenseMatrix(XandY.map { case (sampleVector, sampleLabel) => sampleVector }: _*)
    val Y = DenseVector(XandY.map { case (sampleVector, sampleLabel) => sampleLabel }: _*)
    (X, Y)
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): VM
}

object AdditiveLogisticRegression extends AdditiveLogisticRegression {
  val regressionName = "additiveLogisticRegression"
}

trait AdditiveLogisticRegression extends LogisticSiteRegression[AdditiveLogisticVariantModel] with Additive {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[AdditiveLogisticVariantModel] = {

    // Note: we would like to use a map below, but need some way to deal with singular matrix exceptions being thrown
    // by applyToSite. flatMap unpacks the Some/None objects into the correct product case classes.
    genotypes.flatMap((genos: CalledVariant) => {
      try {
        val association = applyToSite(phenotypes.value, genos)
        Some(constructVM(genos, phenotypes.value.head._2, association))
      } catch {
        case e: breeze.linalg.MatrixSingularException => {
          logError(e.toString)
          None: Option[AdditiveLogisticVariantModel]
        }
      }
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): AdditiveLogisticVariantModel = {
    AdditiveLogisticVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}

object DominantLogisticRegression extends DominantLogisticRegression {
  val regressionName = "dominantLogisticRegression"
}

trait DominantLogisticRegression extends LogisticSiteRegression[DominantLogisticVariantModel] with Dominant {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[DominantLogisticVariantModel] = {

    genotypes.flatMap((genos: CalledVariant) => {
      try {
        val association = applyToSite(phenotypes.value, genos)
        Some(constructVM(genos, phenotypes.value.head._2, association))
      } catch {
        case e: breeze.linalg.MatrixSingularException => {
          logError(e.toString)
          None: Option[DominantLogisticVariantModel]
        }
      }
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): DominantLogisticVariantModel = {
    DominantLogisticVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}
