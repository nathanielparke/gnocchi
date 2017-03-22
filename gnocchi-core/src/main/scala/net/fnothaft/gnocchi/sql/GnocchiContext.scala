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
package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.rdd.association.Association
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.functions._

object GnocchiContext {

  implicit def gcFromSqlContext(sqlContext: SQLContext): GnocchiSqlContext =
    new GnocchiSqlContext(sqlContext)
}

class GnocchiSqlContext private[sql] (@transient sqlContext: SQLContext) extends Serializable {

  import sqlContext.implicits._

  def toGenotypeStateDataset(gtFrame: DataFrame, ploidy: Int): Dataset[GenotypeState] = {
    toGenotypeStateDataFrame(gtFrame, ploidy).as[GenotypeState]
  }

  def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int, sparse: Boolean = false): DataFrame = {

    val filteredGtFrame = if (sparse) {
      // if we want the sparse representation, we prefilter
      val sparseFilter = (0 until ploidy).map(i => {
        gtFrame("alleles").getItem(i) !== "Ref"
      }).reduce(_ || _)
      gtFrame.filter(sparseFilter)
    } else {
      gtFrame
    }

    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "Ref", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "NoCall", 1).otherwise(0)
      c
    }).reduce(_ + _)

    filteredGtFrame.select(filteredGtFrame("variant.contig.contigName").as("contig"),
      filteredGtFrame("variant.start").as("start"),
      filteredGtFrame("variant.end").as("end"),
      filteredGtFrame("variant.referenceAllele").as("ref"),
      filteredGtFrame("variant.alternateAllele").as("alt"),
      filteredGtFrame("sampleId"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"))
  }
}

object AuxEncoders {
  implicit def associationEncoder: org.apache.spark.sql.Encoder[Association] = org.apache.spark.sql.Encoders.kryo[Association]
  implicit def genotypeStateEncoder: org.apache.spark.sql.Encoder[GenotypeState] = org.apache.spark.sql.Encoders.kryo[GenotypeState]

}
