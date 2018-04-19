/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.gnocchi.primitives.variants

import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState

case class CalledVariant(uniqueID: String,
                         chromosome: Int,
                         position: Int,
                         referenceAllele: String,
                         alternateAllele: String,
                         maf: Double,
                         missingness: Double,
                         samples: Map[String, GenotypeState]) extends Product {
  val ploidy: Int = samples.head._2.ploidy
}

object CalledVariant {
  /**
   * @return the minor allele frequency across all samples for this variant
   */
  def maf(calledVariant: CalledVariant): Double = {
    val missingCount = calledVariant.samples.values.map(_.misses.toInt).sum
    val alleleCount = calledVariant.samples.values.map(_.alts.toInt).sum

    assert(calledVariant.samples.size * calledVariant.ploidy > missingCount, s"Variant, ${calledVariant.uniqueID}, has entirely missing row.")

    if (calledVariant.samples.size * calledVariant.ploidy > missingCount) {
      alleleCount.toDouble / (calledVariant.samples.size * calledVariant.ploidy - missingCount).toDouble
    } else {
      0.5
    }
  }

  /**
   * @return The fraction of missing values for this variant values across all samples
   */
  def geno(calledVariant: CalledVariant): Double = {
    val missingCount = calledVariant.samples.values.map(_.misses.toInt).sum

    missingCount.toDouble / (calledVariant.samples.size * calledVariant.ploidy).toDouble
  }

  /**
   * @return Number of samples that have all valid values (none missing)
   */
  def numValidSamples(calledVariant: CalledVariant): Int = {
    calledVariant.samples.values.count(_.misses == 0)
  }

  /**
   * @return Number of samples that have some valid values (could be some missing)
   */
  def numSemiValidSamples(calledVariant: CalledVariant): Int = {
    calledVariant.samples.values.count(_.misses < calledVariant.ploidy)
  }
}