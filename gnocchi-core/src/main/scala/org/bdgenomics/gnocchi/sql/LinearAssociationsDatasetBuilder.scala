package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.Path
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearRegressionResults
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.primitives.association.LinearAssociationBuilder
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession._

case class LinearAssociationsDatasetBuilder(linearAssociationBuilders: Dataset[LinearAssociationBuilder],
                                            phenotypeNames: String,
                                            covariatesNames: List[String],
                                            sampleUIDs: Set[String],
                                            numSamples: Int,
                                            allelicAssumption: String) {

  //  val fullyBuilt = has seen genotypes for all individual ids used to build the model

  import linearAssociationBuilders.sparkSession.implicits._

  /**
   * Joins a [[Dataset]] of [[CalledVariant]] with a [[Dataset]] of [[LinearAssociationBuilder]] and updates
   * the [[LinearAssociationBuilder]] objects. It then constructs a new [[LinearAssociationsDatasetBuilder]]
   * from the resulting merged associations.
   *
   * ToDo: Make sure individuals in new data were part of the originally created model
   *
   * @param newGenotypeData new [[Dataset]] of [[CalledVariant]] to apply the model to and get the sum of squared residuals for
   * @param newPhenotypeData new [[PhenotypesContainer]] object containing the phenotype data corresponding to the
   *                         newGenotypeData param
   * @return a new [[LinearAssociationsDatasetBuilder]] which is updated with the added data
   */
  def addNewData(newGenotypeData: GenotypeDataset,
                 newPhenotypeData: PhenotypesContainer): LinearAssociationsDatasetBuilder = {
    LinearAssociationsDatasetBuilder(
      linearAssociationBuilders
        .joinWith(newGenotypeData.genotypes, linearAssociationBuilders("model.uniqueID") === newGenotypeData.genotypes("uniqueID"))
        .map { case (builder, newVariant) => builder.addNewData(newVariant, newPhenotypeData.phenotypes.value, allelicAssumption) },
      phenotypeNames,
      covariatesNames,
      sampleUIDs,
      numSamples,
      allelicAssumption)
  }

  def saveAssociations(outPath: String): Unit = {
    val sc = linearAssociationBuilders.sparkSession.sparkContext
    sc.saveAssociations(linearAssociationBuilders.map(_.association),
      outPath,
      saveAsText = true)
  }
}

object LinearAssociationsDatasetBuilder {
  def apply(model: LinearGnocchiModel,
            genotypeData: GenotypeDataset,
            phenotypeData: PhenotypesContainer): LinearAssociationsDatasetBuilder = {

    import model.variantModels.sparkSession.implicits._

    val linearAssociationBuilders = model.variantModels.joinWith(genotypeData.genotypes, model.variantModels("uniqueID") === genotypeData.genotypes("uniqueID"))
      .map {
        case (model, genotype) => {
          LinearAssociationBuilder(model, model.createAssociation(genotype, phenotypeData.phenotypes.value, genotypeData.allelicAssumption))
        }
      }
    LinearAssociationsDatasetBuilder(linearAssociationBuilders,
      model.phenotypeNames,
      model.covariatesNames,
      model.sampleUIDs,
      model.numSamples,
      model.allelicAssumption)
  }
}