package org.bdgenomics.gnocchi.algorithms.siteregression

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }

import scala.collection.immutable.Map

case class LinearRegressionResults(genotypes: GenotypeDataset,
                                   phenotypes: PhenotypesContainer) {

  lazy val (models: Dataset[LinearVariantModel], associations: Dataset[LinearAssociation]) =
    LinearSiteRegression.createModelAndAssociations(
      genotypes.genotypes,
      phenotypes.phenotypes,
      allelicAssumption = genotypes.allelicAssumption)

  lazy val gnocchiModel: LinearGnocchiModel = {
    LinearGnocchiModel(
      models,
      phenotypes.phenotypeName,
      phenotypes.covariateNames.getOrElse(List()),
      genotypes.sampleUIDs.toSet,
      phenotypes.numSamples,
      genotypes.allelicAssumption)
  }

  //  def saveAssociations()
  //  def saveVariantModels()
  //  def saveAsGnocchiModel()

}
