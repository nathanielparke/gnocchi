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
package net.fnothaft.gnocchi.models.logistic

import java.io.{ File, FileOutputStream, ObjectOutputStream }

import net.fnothaft.gnocchi.algorithms.siteregression.{ Dominant, DominantLogisticRegression, LogisticSiteRegression }
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.logistic.DominantLogisticVariantModel
import net.fnothaft.gnocchi.primitives.association.LogisticAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.formats.avro.Variant

object DominantLogisticGnocchiModelFactory {

  val regressionName = "DominantLogisticRegression"
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            QCVariantIDs: Option[Set[String]] = None,
            QCVariantSamplingRate: Double = 0.1,
            validationStringency: String = "STRICT"): DominantLogisticGnocchiModel = {

    // ToDo: sampling QC Variants better.
    val variantModels = DominantLogisticRegression(genotypes, phenotypes, validationStringency)

    // Create QCVariantModels
    val comparisonVariants = genotypes.sample(false, QCVariantSamplingRate)
    val QCVariantModels = variantModels
      .joinWith(comparisonVariants, variantModels("uniqueID") === comparisonVariants("uniqueID"), "inner")
      .withColumnRenamed("_1", "variantModel")
      .withColumnRenamed("_2", "variant")
      .as[QualityControlVariantModel[DominantLogisticVariantModel]]

    // Create metadata
    val metadata = GnocchiModelMetaData(regressionName,
      phenotypes.value.head._2.phenoName,
      phenotypes.value.head._2.covariates.mkString(" "),
      genotypes.count().toInt,
      flaggedVariantModels = Option(QCVariantModels.select("variant.uniqueID").as[String].collect().toList))

    DominantLogisticGnocchiModel(metaData = metadata,
      variantModels = variantModels,
      QCVariantModels = QCVariantModels,
      QCPhenotypes = phenotypes.value)
  }
}

case class DominantLogisticGnocchiModel(metaData: GnocchiModelMetaData,
                                        variantModels: Dataset[DominantLogisticVariantModel],
                                        QCVariantModels: Dataset[QualityControlVariantModel[DominantLogisticVariantModel]],
                                        QCPhenotypes: Map[String, Phenotype])
    extends GnocchiModel[DominantLogisticVariantModel, DominantLogisticGnocchiModel] {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  //  def mergeGnocchiModel(otherModel: GnocchiModel[DominantLogisticVariantModel, DominantLogisticGnocchiModel]): GnocchiModel[DominantLogisticVariantModel, DominantLogisticGnocchiModel] = {
  //
  //  }

  def mergeVariantModels(newVariantModels: Dataset[DominantLogisticVariantModel]): Dataset[DominantLogisticVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  }
}
