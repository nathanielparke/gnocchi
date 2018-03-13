package org.bdgenomics.gnocchi.primitives.association

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

case class LinearAssociationBuilder(model: LinearVariantModel,
                                    association: LinearAssociation) {

  def addNewData(genotype: CalledVariant,
                 phenotypes: Map[String, Phenotype]): LinearAssociationBuilder = {

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(genotype, phenotypes, "ADDITIVE")

    val xTx_shaped = new DenseMatrix(model.numPredictors, model.numPredictors, model.xTx)
    val beta = new DenseVector(model.weights.toArray)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(x, y, beta, xTx_shaped, Option(association.SSResiduals), Option(association.numSamples))

    // I don't like having the num samples updated here...
    val newAssociation = LinearAssociation(
      model.uniqueID,
      model.chromosome,
      model.position,
      model.numSamples + x.rows,
      t,
      pValue,
      genoSE,
      ssResiduals)

    LinearAssociationBuilder(model, newAssociation)
  }
}
