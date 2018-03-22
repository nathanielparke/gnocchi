package org.bdgenomics.gnocchi.algorithms.siteregression

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.LogisticGnocchiModel
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }

case class LogisticRegressionResults(genotypes: GenotypeDataset,
                                     phenotypes: PhenotypesContainer) {
  lazy val (models: Dataset[LogisticVariantModel], associations: Dataset[LogisticAssociation]) =
    LogisticSiteRegression.createModelAndAssociations(
      genotypes.genotypes,
      phenotypes.phenotypes,
      allelicAssumption = genotypes.allelicAssumption)

  lazy val gnocchiModel: LogisticGnocchiModel = {
    LogisticGnocchiModel(
      models,
      phenotypes.phenotypeName,
      phenotypes.covariateNames.getOrElse(List()),
      genotypes.sampleUIDs.toSet,
      phenotypes.numSamples,
      genotypes.allelicAssumption)
  }
}
