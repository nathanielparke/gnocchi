package org.bdgenomics.gnocchi.sql

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

case class GenotypeDataset(genotypes: Dataset[CalledVariant])
