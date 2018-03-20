package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ col }
import org.apache.hadoop.fs.Path
import org.bdgenomics.gnocchi.primitives.association.LinearAssociationBuilder
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

case class LinearAssociationsDatasetBuilder(linearAssociationBuilders: Dataset[LinearAssociationBuilder]) {

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
        .map { case (builder, newVariant) => builder.addNewData(newVariant, newPhenotypeData.phenotypes.value) })
  }

  def saveAssociations(outPath: String,
                       forceSave: Boolean = false): Unit = {

    // save dataset
    val associationsFile = new Path(outPath)
    val fs = associationsFile.getFileSystem(linearAssociationBuilders.sparkSession.sparkContext.hadoopConfiguration)
    if (fs.exists(associationsFile)) {
      if (forceSave) {
        fs.delete(associationsFile)
      } else {
        val input = readLine(s"Specified output file ${outPath} already exists. Overwrite? (y/n)> ")
        if (input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes")) {
          fs.delete(associationsFile)
        }
      }
    }

    val necessaryFields = List("uniqueID", "chromosome", "position", "tStatistic", "pValue", "GenotypeStandardError").map(col)

    val assoc = linearAssociationBuilders
      .map(_.association)
      .select(necessaryFields: _*)
      .sort($"pValue".asc)
      .coalesce(1)

    assoc.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(outPath)
  }
}