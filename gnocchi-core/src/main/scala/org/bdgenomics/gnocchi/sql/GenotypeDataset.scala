package org.bdgenomics.gnocchi.sql

import java.io.ObjectOutputStream

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.hadoop.fs.Path

/**
 * Use this object as a container for genomic data stored in [[CalledVariant]] objects. This object is meant to
 * correspond directly to a genomic dataset that exists in a single location. Building joint models over multiple
 * instances of this object should be possible.
 *
 * @param genotypes the dataset of CalledVariant objects associated with this dataset
 * @param datasetUID Unique Identifier for this dataset of genomic variants. A good example for this would be the
 *                   dbGaP study accession ID
 */
case class GenotypeDataset(@transient genotypes: Dataset[CalledVariant],
                           datasetUID: String,
                           allelicAssumption: String,
                           sampleUIDs: Set[String]) {

  def tranformAllelicAssumption(newAllelicAssumption: String): GenotypeDataset = {
    GenotypeDataset(genotypes, datasetUID, newAllelicAssumption, sampleUIDs)
  }

  def save(saveTo: String): Unit = {
    val metadataPath = new Path(saveTo + "/metaData")

    val metadata_fs = metadataPath.getFileSystem(genotypes.sparkSession.sparkContext.hadoopConfiguration)
    val metadata_oos = new ObjectOutputStream(metadata_fs.create(metadataPath))

    metadata_oos.writeObject(this)
    metadata_oos.close

    genotypes.write.parquet(saveTo + "/genotypes")
  }
}
