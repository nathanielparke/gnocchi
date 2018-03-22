package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

/**
 * Use this object as a container for genomic data stored in [[CalledVariant]] objects. This object is meant to
 * correspond directly to a genomic dataset that exists in a single location. Building joint models over multiple
 * instances of this object should be possible.
 *
 * @param genotypes the dataset of CalledVariant objects associated with this dataset
 * @param datasetUID Unique Identifier for this dataset of genomic variants. A good example for this would be the
 *                   dbGaP study accession ID
 */
case class GenotypeDataset(genotypes: Dataset[CalledVariant],
                           datasetUID: String,
                           allelicAssumption: String) {

  // This might not accurately get all the sample UIDs because the head might not contain all samples...
  lazy val sampleUIDs: List[String] = genotypes.head.samples.map(f => datasetUID + "_" + f.sampleID)

  //  def tranformAllelicAssumption(newAllelicAssumption: String): GenotypeDataset = {
  //    genotypes.map(f => {
  //
  //    })
  //  }
}
