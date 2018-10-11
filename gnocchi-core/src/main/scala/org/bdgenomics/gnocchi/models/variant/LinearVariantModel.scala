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

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

/**
 * Data container for the statistical model produced by running [[LinearSiteRegression]] on a single
 * variant. This model can be merged with another [[LinearVariantModel]] that was built over
 * separate data, to produce a model that is analytically equivalent to building a single model over
 * the union of the two datasets.
 *
 * @param uniqueID Unique identifier of the variant this model is associated with
 * @param chromosome Chromosome of the variant this model is associated with
 * @param position Position of the variant this model is associated with
 * @param referenceAllele Reference allele of the variant this model is associated with
 * @param alternateAllele Alternate allele of the variant this model is associated with
 * @param numSamples Number of samples used to build this model
 * @param numPredictors Number of variables in the statistical model
 * @param xTx a numPredictors x numPredictors sized matrix (stored as an array for serialization)
 *            that is an smaller representation of the original design matrix used in the regression
 * @param xTy a numPredictor length vector that is a smaller representation of the original labels
 *            vector used in the regression
 * @param residualDegreesOfFreedom The degrees of freedom of the Linear model. This is calculated as
 *                                 the number of samples minus the number of predictors (bias term
 *                                 included)
 * @param weights The weights of the resulting Linear model
 */
case class LinearVariantModel(uniqueID: String,
                              chromosome: Int,
                              position: Int,
                              referenceAllele: String,
                              alternateAllele: String,
                              numSamples: Int,
                              numPredictors: Int,
                              xTx: Array[Double],
                              xTy: Array[Double],
                              residualDegreesOfFreedom: Int,
                              weights: List[Double])
    extends VariantModel[LinearVariantModel] with LinearSiteRegression