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
package org.bdgenomics.gnocchi.algorithms.siteregression

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }

/**
 * Results container for results from a [[LinearSiteRegression]] Analysis
 *
 * @param genotypes [[GenotypeDataset]] used to generate the results
 * @param phenotypes [[PhenotypesContainer]] corresponding to the genotype data
 */
case class LinearRegressionResults(genotypes: GenotypeDataset,
                                   phenotypes: PhenotypesContainer) {

  lazy val (models: Dataset[LinearVariantModel], associations: Dataset[LinearAssociation]) =
    LinearSiteRegression.createModelAndAssociations(
      genotypes.genotypes,
      phenotypes.phenotypes,
      allelicAssumption = genotypes.allelicAssumption)

  lazy val gnocchiModel: LinearGnocchiModel = {
    LinearGnocchiModel(
      models,
      phenotypes.phenotypeName,
      phenotypes.covariateNames.getOrElse(List()),
      genotypes.sampleUIDs.toSet,
      phenotypes.numSamples,
      genotypes.allelicAssumption)
  }

  //  def saveAssociations()
  //  def saveVariantModels()
  //  def saveAsGnocchiModel()

}
