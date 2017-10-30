package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LinearVariantModel }
import org.bdgenomics.gnocchi.models.{ GnocchiModelMetaData, GnocchiModel, LinearGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

// (TODO) Add factory apply

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