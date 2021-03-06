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
package org.bdgenomics.gnocchi.models.variant

import org.bdgenomics.gnocchi.algorithms.siteregression.LogisticSiteRegression

/**
 * Data container for the statistical model produced by running [[LogisticSiteRegression]] on a
 * single variant.
 *
 * @param uniqueID Unique identifier of the variant this model is associated with
 * @param chromosome Chromosome of the variant this model is associated with
 * @param position Position of the variant this model is associated with
 * @param referenceAllele Reference allele of the variant this model is associated with
 * @param alternateAllele Alternate allele of the variant this model is associated with
 * @param weights the weights of the Logistic model that were found through training in
 *                [[LogisticSiteRegression]]
 */
case class LogisticVariantModel(uniqueID: String,
                                chromosome: Int,
                                position: Int,
                                referenceAllele: String,
                                alternateAllele: String,
                                weights: List[Double])
    extends VariantModel[LogisticVariantModel] with LogisticSiteRegression