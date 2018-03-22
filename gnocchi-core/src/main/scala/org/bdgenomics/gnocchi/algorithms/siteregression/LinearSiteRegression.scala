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

import org.bdgenomics.gnocchi.primitives.association.{ LinearAssociation, LinearAssociationBuilder }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.StudentsT
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }

import scala.collection.immutable.Map

trait LinearSiteRegression extends SiteRegression[LinearVariantModel, LinearAssociation] {

  /**
   * Default apply which creates two datasets, one for the model, one for the results
   *
   * @param genotypes
   * @param phenotypesContainer
   * @param allelicAssumption
   * @return
   */
  def apply(genotypes: GenotypeDataset,
            phenotypesContainer: PhenotypesContainer): LinearRegressionResults = {
    LinearRegressionResults(genotypes, phenotypesContainer)
  }

  def createModelAndAssociations(genotypes: Dataset[CalledVariant],
                                 phenotypes: Broadcast[Map[String, Phenotype]],
                                 allelicAssumption: String): (Dataset[LinearVariantModel], Dataset[LinearAssociation]) = {

    import genotypes.sqlContext.implicits._

    //ToDo: Singular Matrix Exceptions
    val results = genotypes.flatMap((genos: CalledVariant) => {
      try {
        val (model, association) = applyToSite(phenotypes.value, genos, allelicAssumption)
        Some((model, association))
      } catch {
        case e: breeze.linalg.MatrixSingularException => None
      }
    })

    (results.map(_._1), results.map(_._2))
  }

  /**
   * Apply to site.
   *
   * @param phenotypes
   * @param genotypes
   * @param allelicAssumption
   * @return
   */
  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant,
                  allelicAssumption: String): (LinearVariantModel, LinearAssociation) = {

    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, allelicAssumption)

    val xTx = x.t * x // p x p matrix
    val xTy = x.t * y // p x 1 vector

    val beta = xTx \ xTy

    val (genoSE, t, pValue, ssResiduals) = calculateSignificance(x, y, beta, xTx)

    val association = LinearAssociation(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      x.rows,
      pValue,
      genoSE,
      ssResiduals,
      t)

    val model = LinearVariantModel(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      genotypes.referenceAllele,
      genotypes.alternateAllele,
      x.rows,
      x.cols,
      xTx.toArray,
      xTy.toArray,
      x.rows - x.cols,
      beta.data.toList)

    (model, association)
  }

  /**
   *
   * @param x
   * @param y
   * @param beta
   * @param modelxTx
   * @param partialSSResiduals Optional parameter that can be used as the SSResiduals from another/other datasets
   * @return (genotype parameter standard error, t-statistic for model, pValue for model, sum of squared residuals)
   */
  def calculateSignificance(x: DenseMatrix[Double],
                            y: DenseVector[Double],
                            beta: DenseVector[Double],
                            modelxTx: DenseMatrix[Double],
                            partialSSResiduals: Option[Double] = None,
                            additionalSamples: Option[Int] = None): (Double, Double, Double, Double) = {

    require(partialSSResiduals.isDefined == additionalSamples.isDefined,
      "You need to either define both partialSSResiduals and additionalNumSamples, or neither.")

    val residuals = y - (x * beta)
    val ssResiduals = residuals.t * residuals + partialSSResiduals.getOrElse(0: Double)

    // compute the regression parameters standard errors
    val betaVariance = diag(inv(modelxTx))
    val sigma = ssResiduals / (additionalSamples.getOrElse(0: Int) + x.rows - x.cols)
    val standardErrors = sqrt(sigma * betaVariance)

    // get standard error for genotype parameter (for p value calculation)
    val genoSE = standardErrors(1)

    // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
    val t = beta(1) / genoSE

    /* calculate p-value and report:
      Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
      a t-distribution with N-p degrees of freedom.
      (N = number of samples, p = number of regressors i.e. genotype+covariates+intercept)
      https://en.wikipedia.org/wiki/T-statistic
    */
    val residualDegreesOfFreedom = additionalSamples.getOrElse(0: Int) + x.rows - x.cols
    val tDist = StudentsT(residualDegreesOfFreedom)
    val pValue = 2 * tDist.cdf(-math.abs(t))
    (genoSE, t, pValue, ssResiduals)
  }

  /**
   * Take a CalledVariant and turn it into a Breeze Matrix and DenseVector.
   *
   * @param genotypes
   * @param phenotypes
   * @param allelicAssumption
   * @return
   */
  //  private[algorithms] def prepareDesignMatrix_covar(genotypes: CalledVariant,
  def prepareDesignMatrix(genotypes: CalledVariant,
                          phenotypes: Map[String, Phenotype],
                          allelicAssumption: String): (DenseMatrix[Double], DenseVector[Double]) = {

    val validGenos = genotypes.samples.filter(genotypeState => genotypeState.misses == 0 && phenotypes.contains(genotypeState.sampleID))

    val samplesGenotypes = allelicAssumption.toUpperCase match {
      case "ADDITIVE"  => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.additive))
      case "DOMINANT"  => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.dominant))
      case "RECESSIVE" => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.recessive))
    }

    val (primitiveX, primitiveY) = samplesGenotypes.flatMap({
      case (sampleID, genotype) if phenotypes.contains(sampleID) => {
        Some(1.0 +: genotype +: phenotypes(sampleID).covariates.toArray, phenotypes(sampleID).phenotype)
      }
      case _ => None
    }).toArray.unzip

    if (primitiveX.length == 0) {
      // TODO: Determine what to do when the design matrix is empty (i.e. no overlap btwn geno and pheno sampleIDs, etc.)
      throw new IllegalArgumentException("No overlap between phenotype and genotype state sample IDs.")
    }

    // NOTE: This may cause problems in the future depending on JVM max varargs, use one of these instead if it breaks:
    // val x = new DenseMatrix(x(0).length, x.length, x.flatten).t
    // val x = new DenseMatrix(x.length, x(0).length, x.flatten, 0, x(0).length, isTranspose = true)
    // val x = new DenseMatrix(x :_*)

    (new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten), new DenseVector(primitiveY))
  }
}

object LinearSiteRegression extends LinearSiteRegression