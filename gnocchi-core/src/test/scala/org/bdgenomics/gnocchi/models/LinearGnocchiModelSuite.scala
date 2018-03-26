package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.mockito.Mockito

import scala.collection.mutable

class LinearGnocchiModelSuite extends GnocchiFunSuite {
  ignore("Unit test of LGM.mergeVariantModels") {
    // (TODO) To unit test mergeVariantModels requires a seperate constructor that takes a mock of Dataset[LinearVariantModel] which is fairly messy
  }

  ignore("LinearGnocchiModel correctly combines GnocchiModels") {

  }

  ignore("LinearGnocchiModel correctly combines greater than 2 GnocchiModels") {

  }

  ignore("LinearGnocchiModel.mergeQCVariants correct combines variant samples") {

  }
}
