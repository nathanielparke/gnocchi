package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LinearVariantModel }
import org.bdgenomics.gnocchi.models.{ LinearGnocchiModelFactory, GnocchiModel, LinearGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.JavaConversions._

object JavaLinearGnocchiModelFactory {
  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[java.lang.String, Phenotype]],
            phenotypeNames: java.util.ArrayList[java.lang.String], // Option becomes raw object java.util.ArrayList[java.lang.String],
            QCVariantIDs: java.util.ArrayList[java.lang.String], // Option becomes raw object
            QCVariantSamplingRate: java.lang.Double = 0.1,
            allelicAssumption: java.lang.String = "ADDITIVE",
            validationStringency: java.lang.String = "STRICT"): LinearGnocchiModel = {

    // Convert python compatible nullable types to scala options
    val phenotypeNamesOption = if (phenotypeNames == null) {
      None
    } else {
      val phenotypeNamesList = asScalaBuffer(phenotypeNames).toList
      Some(phenotypeNamesList)
    }

    val QCVariantIDsOption = if (QCVariantIDs == null) {
      None
    } else {
      val QCVariantIDsList = asScalaBuffer(QCVariantIDs).toSet
      Some(QCVariantIDsList)
    }

    LinearGnocchiModelFactory(genotypes,
      phenotypes,
      phenotypeNamesOption,
      QCVariantIDsOption,
      QCVariantSamplingRate,
      allelicAssumption,
      validationStringency)
  }
}

class JavaLinearGnocchiModel(val lgm: LinearGnocchiModel) {
  def mergeGnocchiModel(otherModel: GnocchiModel[LinearVariantModel, LinearGnocchiModel]): GnocchiModel[LinearVariantModel, LinearGnocchiModel] = {
    lgm.mergeGnocchiModel(otherModel)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    lgm.mergeVariantModels(newVariantModels)
  }

  def mergeQCVariants(newQCVariantModels: Dataset[QualityControlVariantModel[LinearVariantModel]]): Dataset[CalledVariant] = {
    lgm.mergeQCVariants(newQCVariantModels)
  }
}