package org.bdgenomics.gnocchi.sql

import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype

/**
 * Container object
 *
 * @param phenotypes
 * @param phenotypeName
 * @param covariateNames
 */
case class PhenotypesContainer(phenotypes: Broadcast[Map[String, Phenotype]],
                               phenotypeName: String,
                               covariateNames: Option[List[String]]) {
  val numSamples = phenotypes.value.size
}