package org.bdgenomics.gnocchi.models

case class ResultWrapper(geneticParameterStandardError: Double,
                         tStatistic: Double,
                         pValue: Double)