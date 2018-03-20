package org.bdgenomics.gnocchi.api.java.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel

class JavaLinearGnocchiModel(val lgm: LinearGnocchiModel) {
  def mergeGnocchiModel(otherModel: JavaLinearGnocchiModel): JavaLinearGnocchiModel = {
    val newModel = lgm.mergeGnocchiModel(otherModel.lgm).asInstanceOf[LinearGnocchiModel]
    new JavaLinearGnocchiModel(newModel)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    lgm.mergeVariantModels(newVariantModels)
  }

  def getVariantModels(): Dataset[LinearVariantModel] = {
    lgm.variantModels
  }

  def getModelType(): java.lang.String = {
    lgm.allelicAssumption
  }

  def getPhenotype(): java.lang.String = {
    lgm.phenotypeNames
  }

  def getCovariates(): java.lang.String = {
    lgm.covariatesNames.mkString(",")
  }

  def getNumSamples(): java.lang.Integer = {
    lgm.numSamples
  }

  //  def save(saveTo: java.lang.String): Unit = {
  //
  //    sc.save()
  //  }
}
