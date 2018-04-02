package org.bdgenomics.gnocchi.utils

object AllelicAssumption extends Enumeration {
  type AllelicAssumption = Value
  val additive, dominant, recessive = Value
}
