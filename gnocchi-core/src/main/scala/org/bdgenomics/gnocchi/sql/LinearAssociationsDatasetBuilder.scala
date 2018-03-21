package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ col }
import org.apache.hadoop.fs.Path
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

  val sc = linearAssociationBuilders.sparkSession.sparkContext

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

  def saveAssociations(outPath: String,
                       forceSave: Boolean = false): Unit = {
    sc.saveAssociations(linearAssociationBuilders.map(_.association),
      outPath,
      forceSave = forceSave,
      saveAsText = true)
  }
}