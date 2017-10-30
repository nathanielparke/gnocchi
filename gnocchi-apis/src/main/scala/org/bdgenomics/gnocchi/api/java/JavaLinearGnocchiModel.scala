package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LinearVariantModel }
import org.bdgenomics.gnocchi.models.{ LinearGnocchiModelFactory, GnocchiModel, LinearGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

object JavaLinearGnocchiModelFactory {
  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            phenotypeNames: Option[List[String]],
            QCVariantIDs: Option[Set[String]] = None,
            QCVariantSamplingRate: java.lang.Double = 0.1,
            allelicAssumption: java.lang.String = "ADDITIVE",
            validationStringency: java.lang.String = "STRICT"): LinearGnocchiModel = {
    LinearGnocchiModelFactory(genotypes,
      phenotypes,
      phenotypeNames,
      QCVariantIDs,
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