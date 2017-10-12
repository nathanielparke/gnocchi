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

import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel, LinearVariantModel }
import net.fnothaft.gnocchi.primitives.association.LinearAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.StudentsT
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.collection.immutable.Map
import scala.math.log10

trait LinearSiteRegression[VM <: LinearVariantModel[VM]] extends SiteRegression[VM] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[VM]

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): LinearAssociation = {
    val (x, y) = prepareDesignMatrix(genotypes, phenotypes).unzip
    // NOTE: This may cause problems in the future depending on JVM max varargs, use one of these instead if it breaks:
    // val matX = new DenseMatrix(x(0).length, x.length, x.flatten).t
    val matX = new DenseMatrix(x.length, x(0).length, x.flatten, 0, x(0).length, isTranspose = true)
    // val matX = new DenseMatrix(x :_*)
    val vecY = new DenseVector(y)

    try {
      // TODO: Determine if QR factorization is faster
      val result = matX \ vecY

      val residuals = vecY - (matX * result)
      val ssResiduals = norm(residuals)

      // calculate sum of squared deviations
      val genos = matX(::, 0)
      val genoMean = sum(genos) / matX.rows
      val ssDeviations = sum(pow((genos - genoMean), 2))

      // compute the regression parameters standard errors
      val betaVariance = diag(inv(matX.t * matX))
      val sigma = residuals.t * residuals / (matX.rows - matX.cols)
      val standardErrors = sqrt(sigma * betaVariance)

      // get standard error for genotype parameter (for p value calculation)
      val genoSE = standardErrors(1)

      // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
      val t = result(1) / genoSE

      /* calculate p-value and report:
        Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
        a t-distribution with N-p-1 degrees of freedom. (N = number of samples, p = number of regressors i.e. genotype+covariates+intercept)
        https://en.wikipedia.org/wiki/T-statistic
      */
      val residualDegreesOfFreedom = matX.rows - matX.cols - 1
      val tDist = StudentsT(residualDegreesOfFreedom)
      val pValue = 2 * tDist.cdf(-math.abs(t))
      val logPValue = log10(pValue)

      LinearAssociation(
        ssDeviations,
        ssResiduals,
        genoSE,
        t,
        residualDegreesOfFreedom,
        pValue,
        result.data.toList,
        genotypes.numValidSamples)
    } catch {
      case _: MatrixSingularException => {
        // TODO: Rethrow for now, I don't remember what this was originally for...
        throw new MatrixSingularException()
      }
    }
  }

  private def prepareDesignMatrix(genotypes: CalledVariant,
                                  phenotypes: Map[String, Phenotype]): Array[(Array[Double], Double)] = {
    val filteredGenotypes = genotypes.samples.filter(!_.value.contains("."))

    filteredGenotypes.map(gs => {
      val pheno = phenotypes(gs.sampleID)
      (1.0 +: clipOrKeepState(gs.toDouble) +: pheno.covariates.toArray, pheno.phenotype)
    }).toArray
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): VM
}

object AdditiveLinearRegression extends AdditiveLinearRegression {
  val regressionName = "additiveLinearRegression"
}

trait AdditiveLinearRegression extends LinearSiteRegression[AdditiveLinearVariantModel] with Additive {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[AdditiveLinearVariantModel] = {

    //ToDo: Singular Matrix Exceptions
    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): AdditiveLinearVariantModel = {
    AdditiveLinearVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}

object DominantLinearRegression extends DominantLinearRegression {
  val regressionName = "dominantLinearRegression"
}

trait DominantLinearRegression extends LinearSiteRegression[DominantLinearVariantModel] with Dominant {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[DominantLinearVariantModel] = {

    //ToDo: Singular Matrix Exceptions
    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): DominantLinearVariantModel = {
    DominantLinearVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}
