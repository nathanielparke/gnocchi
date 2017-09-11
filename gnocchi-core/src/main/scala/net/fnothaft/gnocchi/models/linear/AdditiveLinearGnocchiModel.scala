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

import java.io.{File, FileOutputStream, ObjectOutputStream, PrintWriter}

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bdgenomics.formats.avro.Variant

import scala.pickling.Defaults._
import scala.pickling.json._

case class AdditiveLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: Dataset[AdditiveLinearVariantModel],
                                      comparisonVariants: Dataset[CalledVariant],
                                      QCPhenotypes: Map[String, Phenotype])
    extends GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] {

  val QCVariantModels = variantModels
    .joinWith(comparisonVariants, variantModels("uniqueID") === comparisonVariants("uniqueID"), "inner")
    .withColumnRenamed("_1", "variantModel")
    .withColumnRenamed("_2", "variant")
    .as[QualityControlVariantModel[AdditiveLinearVariantModel]]


  def mergeGnocchiModel(newSamples: Dataset[CalledVariant],
                        newPhenotypes: Broadcast[Map[String, Phenotype]]): GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] = {

    val newVMs = AdditiveLinearRegression(newSamples, newPhenotypes)
    val mergedVMs = mergeVariantModels(newVMs)
    val updatedMetaData = updateMetaData(newSamples.count().toInt)
    val updatedQCVariantModels = updateQCVariantModels()

    AdditiveLinearGnocchiModel(updatedMetaData, mergedVMs, updatedQCVariantModels)
  }

  def updateQCVariantModels(): Dataset[QualityControlVariantModel[AdditiveLinearVariantModel]]

  def mergeVariantModels(newVariantModels: Dataset[AdditiveLinearVariantModel]): Dataset[AdditiveLinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  }

  def createQCVariantModels(variants: Dataset[CalledVariant],
                            phenotypes: Map[String, Phenotype]): Dataset[QualityControlVariantModel[AdditiveLinearVariantModel]] = {
    // Sample some variants
    // Run the regression on the sites to give back the variant models for each of the
    //
  }

  def save(saveTo: String): Unit = {
//    val sparkSession = SparkSession.builder().getOrCreate()
//    import sparkSession.implicits._
//    import net.fnothaft.gnocchi.sql.AuxEncoders._
//    sparkSession.createDataset(variantModels).toDF.write.parquet(saveTo + "/variantModels")
//    sparkSession.createDataset(comparisonVariantModels.map(vmobs => {
//      val (vm, observations) = vmobs
//      new QualityControlVariantModel[AdditiveLinearVariantModel](vm, observations)
//    }))
//      .toDF.write.parquet(saveTo + "/qcModels")
//
//    metaData.save(saveTo + "/metaData")
  }
}
