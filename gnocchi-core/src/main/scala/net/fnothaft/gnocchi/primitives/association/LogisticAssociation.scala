package net.fnothaft.gnocchi.primitives.association

case class LogisticAssociation(variantId: String,
                               weights: List[Double],
                               geneticParameterStandardError: Double,
                               pValue: Double,
                               numSamples: Int) extends Association