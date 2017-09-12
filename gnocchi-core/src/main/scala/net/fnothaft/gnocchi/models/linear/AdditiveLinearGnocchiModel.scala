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
package net.fnothaft.gnocchi.models.linear

import java.io.{ File, FileOutputStream, ObjectOutputStream, PrintWriter }

import net.fnothaft.gnocchi.algorithms.siteregression.{ Additive, AdditiveLinearRegression, LinearSiteRegression }
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.primitives.association.LinearAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.formats.avro.Variant

import scala.collection.immutable.Map
import scala.pickling.Defaults._
import scala.pickling.json._

object AdditiveLinearGnocchiModelFactory {

  val regressionName = "additiveLinearRegression"
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            QCVariantIDs: Option[Set[String]] = None,
            QCVariantSamplingRate: Double = 0.1,
            validationStringency: String = "STRICT"): AdditiveLinearGnocchiModel = {

    // ToDo: sampling QC Variants better.
    val variantModels = AdditiveLinearRegression(genotypes, phenotypes, validationStringency)

    // Create QCVariantModels
    val comparisonVariants = genotypes.sample(false, 0.1)
    val QCVariantModels = variantModels
      .joinWith(comparisonVariants, variantModels("uniqueID") === comparisonVariants("uniqueID"), "inner")
      .withColumnRenamed("_1", "variantModel")
      .withColumnRenamed("_2", "variant")
      .as[QualityControlVariantModel[AdditiveLinearVariantModel]]

    // Create metadata
    val metadata = GnocchiModelMetaData(regressionName,
      phenotypes.value.head._2.phenoName,
      phenotypes.value.head._2.covariates.mkString(" "),
      genotypes.count().toInt,
      flaggedVariantModels = Option(QCVariantModels.select("variant.uniqueID").as[String].collect().toList))

    AdditiveLinearGnocchiModel(metaData = metadata,
      variantModels = variantModels,
      QCVariantModels = QCVariantModels,
      QCPhenotypes = phenotypes.value)
  }
}

case class AdditiveLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: Dataset[AdditiveLinearVariantModel],
                                      QCVariantModels: Dataset[QualityControlVariantModel[AdditiveLinearVariantModel]],
                                      QCPhenotypes: Map[String, Phenotype])
    extends GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] {

  def mergeGnocchiModel(otherModel: GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel]): GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] = {

    val mergedVMs = mergeVariantModels(otherModel.variantModels)

    // ToDo: 1. make sure models are of same type 2. find intersection of QCVariants and use those as the gnocchiModel
    // ToDo: QCVariants 3. Make sure the phenotype of the models are the same 4. Make sure the covariates of the model
    // ToDo: are the same (currently broken because covariates stored in [[Phenotype]] object are the values not names)
    val updatedMetaData = updateMetaData(otherModel.metaData.numSamples)

    // AAHH! How do we ensure that variants being compared are the same?
    val updatedQCVariantModels = updateQCVariantModels()

    AdditiveLinearGnocchiModel(updatedMetaData, mergedVMs, updatedQCVariantModels)
  }

  def mergeVariantModels(newVariantModels: Dataset[AdditiveLinearVariantModel]): Dataset[AdditiveLinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  }

//  def updateQCVariantModels(newComparisonVariants: Dataset[CalledVariant],
//                           ): Dataset[QualityControlVariantModel[AdditiveLinearVariantModel]]

//  def createQCVariantModels(variants: Dataset[CalledVariant],
//                            phenotypes: Map[String, Phenotype]): Dataset[QualityControlVariantModel[AdditiveLinearVariantModel]] = {
//    // Sample some variants
//    // Run the regression on the sites to give back the variant models for each of the
//    //
//  }
}
