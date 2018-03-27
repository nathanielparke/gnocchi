package org.bdgenomics.gnocchi.primitives.association

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

/**
 * An object used to store the incremental results of applying a pre-built model to a number of datasets. This object
 * both stores the model that will be applied to each constituent dataset, and the partially built associations of all
 * datasets that the model has been applied to up to this point.
 *
 * @param model a fixed pre-built model that does not change over the course of adding new data to this builder
 * @param association the current state of the association in its incrementally applied form
 */
case class LinearAssociationBuilder(model: LinearVariantModel,
                                    association: LinearAssociation) {

  def addNewData(genotype: CalledVariant,
                 phenotypes: Map[String, Phenotype],
                 allelicAssumption: String): LinearAssociationBuilder = {

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(genotype, phenotypes, allelicAssumption)

    val xTx_shaped = new DenseMatrix(model.numPredictors, model.numPredictors, model.xTx)
    val beta = new DenseVector(model.weights.toArray)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(x, y, beta, xTx_shaped, Option(association.ssResiduals), Option(association.numSamples))

    // I don't like having the num samples updated here...
    val newAssociation =
      LinearAssociation(
        model.uniqueID,
        model.chromosome,
        model.position,
        model.numSamples + x.rows,
        pValue,
        genoSE,
        ssResiduals,
        t)

    LinearAssociationBuilder(model, newAssociation)
  }
}
