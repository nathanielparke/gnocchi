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
package org.bdgenomics.gnocchi.algorithms.siteregression

import breeze.linalg._
import breeze.numerics._
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }

import scala.annotation.tailrec
import scala.collection.immutable.Map

trait LogisticSiteRegression extends SiteRegression[LogisticVariantModel, LogisticAssociation] {

  /**
   *
   * @param genotypes
   * @param phenotypesContainer
   * @return
   */
  def apply(genotypes: GenotypeDataset,
            phenotypesContainer: PhenotypesContainer): LogisticRegressionResults = {
    LogisticRegressionResults(genotypes, phenotypesContainer)
  }

  def createModelAndAssociations(genotypes: Dataset[CalledVariant],
                                 phenotypes: Broadcast[Map[String, Phenotype]],
                                 allelicAssumption: String): (Dataset[LogisticVariantModel], Dataset[LogisticAssociation]) = {
    import genotypes.sqlContext.implicits._

    val results = genotypes.flatMap((genos: CalledVariant) => {
      try {
        val (model, association) = applyToSite(phenotypes.value, genos, allelicAssumption)
        Some((model, association))
      } catch {
        case e: breeze.linalg.MatrixSingularException => {
          logError(e.toString)
          None
        }
        case e: org.apache.commons.math3.exception.ConvergenceException => {
          logError(e.toString)
          None
        }
      }
    })

    (results.map(_._1), results.map(_._2))
  }

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant,
                  allelicAssumption: String): (LogisticVariantModel, LogisticAssociation) = {

    // ToDo: Orthogonalize the matrix so we dont get singular matrices
    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, allelicAssumption)

    val (beta, hessian) = solveRegression(x, y)

    val (genoSE, pValue) = calculateSignificance(x, y, beta, hessian)

    val association = LogisticAssociation(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      x.rows,
      pValue,
      genoSE)

    val model = LogisticVariantModel(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      genotypes.referenceAllele,
      genotypes.alternateAllele,
      beta.toArray.toList)

    (model, association)
  }

  def solveRegression(x: DenseMatrix[Double],
                      y: DenseVector[Double]): (DenseVector[Double], DenseMatrix[Double]) = {
    val maxIter = 1000
    val tolerance = 1e-6
    val initBeta = DenseVector.zeros[Double](x.cols)

    val (beta, hessian) = findBeta(x, y, initBeta, maxIter = maxIter, tolerance = tolerance)
    (DenseVector(beta), hessian)
  }

  /**
   * Tail recursive training function that finds the optimal weights vector given the input training data.
   *
   * @note DO NOT place any statements after the final recursive call to itself, or it will break tail recursive speed
   *       up provided by the scala compiler.
   *
   * @param X [[breeze.linalg.DenseMatrix]] design matrix of [[Double]] that contains training data
   * @param Y [[breeze.linalg.DenseVector]] of labels that contain labels for parameter X
   * @param beta Weights vector
   * @param iter current iteration, used for recursive tracking
   * @param maxIter maximum number of iterations to be used for recursive depth limiting
   * @param tolerance smallest allowable step size before function
   * @return tuple where first item are weight values, beta, as [[Array]]
   *         and second is Hessian matrix as [[DenseMatrix]]
   */
  @tailrec
  final def findBeta(X: DenseMatrix[Double],
                     Y: DenseVector[Double],
                     beta: DenseVector[Double],
                     iter: Int = 0,
                     maxIter: Int = 1000,
                     tolerance: Double = 1e-6): (Array[Double], DenseMatrix[Double]) = {

    val logitArray = X * beta

    // column vector containing probabilities of samples being in class 1 (a case / affected / a positive indicator)
    val p = sigmoid(logitArray)

    // (Xi is a single sample's row) Xi.T * Xi * pi * (1 - pi) is a nXn matrix, that we sum across all i
    val hessian = p.toArray.zipWithIndex.map { case (pi, i) => -X(i, ::).t * X(i, ::) * pi * (1.0 - pi) }.reduce(_ + _)

    // subtract predicted probability from actual response and multiply each row by the error for that sample. Achieved
    // by getting error (Y-p) and copying it columnwise N times (N = number of columns in X) and using *:* to pointwise
    // multiply the resulting matrix with X
    val sampleScore = { X *:* tile(Y - p, 1, X.cols) }

    // sum into one column
    val score = sum(sampleScore(::, *)).t

    val update = -inv(hessian) * score
    val updatedBeta = beta + update

    if (updatedBeta.exists(_.isNaN)) logError("LOG_REG - Broke on iteration: " + iter) // ToDo: deal with this better. Probably need to break / retry with an orthogonal matrix
    if (max(abs(update)) <= tolerance || iter + 1 == maxIter) return (updatedBeta.toArray, hessian)

    findBeta(X, Y, updatedBeta, iter = iter + 1, maxIter = maxIter, tolerance = tolerance)
  }

  def calculateSignificance(x: DenseMatrix[Double],
                            y: DenseVector[Double],
                            beta: DenseVector[Double],
                            hessian: DenseMatrix[Double]): (Double, Double) = {
    val fisherInfo = -hessian
    val fishInv = inv(fisherInfo)
    val standardErrors = sqrt(abs(diag(fishInv)))
    val genoStandardError = standardErrors(1)

    // calculate Wald statistic for each parameter in the regression model
    val zScores: DenseVector[Double] = beta /:/ standardErrors
    val waldStats = zScores *:* zScores

    // calculate cumulative probs
    val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
    val probs = waldStats.map(zi => chiDist.cumulativeProbability(zi))

    val waldTests = 1d - probs
    (genoStandardError, waldTests(1))
  }
}

object LogisticSiteRegression extends LogisticSiteRegression