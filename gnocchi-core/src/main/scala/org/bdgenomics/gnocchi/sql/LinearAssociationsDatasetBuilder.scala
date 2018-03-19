package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.association.LinearAssociationBuilder
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

case class LinearAssociationsDatasetBuilder(linearAssociationBuilders: Dataset[LinearAssociationBuilder]) {

//  val fullyBuilt = has seen genotypes for all individual ids used to build the model

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
  def addNewData(newGenotypeData: Dataset[CalledVariant],
                 newPhenotypeData: PhenotypesContainer): LinearAssociationsDatasetBuilder = {

    LinearAssociationsDatasetBuilder(
      linearAssociationBuilders
      .joinWith(newGenotypeData, linearAssociationBuilders("model.uniqueID") === newGenotypeData("uniqueID"))
      .map { case (builder, newVariant) => builder.addNewData(newVariant, newPhenotypeData.phenotypes.value) }
    )
  }

//  def saveAssociations()
}