package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.mockito.Mockito

import scala.collection.mutable

class LogisticGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("LogisticGnocchiModel creation works") {

  }
}
