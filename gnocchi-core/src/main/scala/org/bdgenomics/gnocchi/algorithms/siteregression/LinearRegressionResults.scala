package org.bdgenomics.gnocchi.algorithms.siteregression

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.PhenotypesContainer

import scala.collection.immutable.Map

case class LinearRegressionResults(genotypes: Dataset[CalledVariant],
                                   phenotypes: PhenotypesContainer,
                                   allelicAssumption: String = "ADDITIVE",
                                   validationStringency: String = "STRICT") {

  lazy val (models: Dataset[LinearVariantModel], associations: Dataset[LinearAssociation]) =
    LinearSiteRegression.createModelAndAssociations(
      genotypes,
      phenotypes.phenotypes,
      allelicAssumption,
      validationStringency)

  lazy val gnocchiModel: LinearGnocchiModel = {
    LinearGnocchiModel(
      models,
      phenotypes.phenotypeName,
      phenotypes.covariateNames.getOrElse(List()),
      phenotypes.numSamples)
  }

  //  def saveAssociations()
  //  def saveVariantModels()
  //  def saveAsGnocchiModel()

}
