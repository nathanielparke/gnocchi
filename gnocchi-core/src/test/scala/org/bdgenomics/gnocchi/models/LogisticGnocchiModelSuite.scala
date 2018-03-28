package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.mockito.Mockito

import scala.collection.mutable

class LogisticGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("LogisticGnocchiModel creation works") {
    val ss = sc.sparkSession
    import ss.implicits._

    val variantModel =
      LogisticVariantModel(
        "rs8330247",
        14,
        21373362,
        "C",
        "T",
        List(0.21603874149667546, -0.20074885895765007, 0.21603874149667546, -0.20074885895765007))

    val variantModels = ss.createDataset(List(variantModel))

    try {
      LogisticGnocchiModel(
        variantModels,
        "pheno_1",
        List("covar_1", "covar_2"),
        Set("6432", "8569", "2411", "6238", "6517", "1939", "6571", "8414", "8327", "6629", "6582", "5600", "7276", "4288", "1890"),
        15,
        "ADDITIVE")
    } catch {
      case _: Throwable => fail("Error creating a LogisticGnocchiModel")
    }
  }
}
