package org.bdgenomics.gnocchi.api.java.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ LogisticVariantModel }
import org.bdgenomics.gnocchi.models.{ LogisticGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

class JavaLogisticGnocchiModel(val lgm: LogisticGnocchiModel) {
  def save(saveTo: java.lang.String): Unit = {
    lgm.save(saveTo)
  }
}
