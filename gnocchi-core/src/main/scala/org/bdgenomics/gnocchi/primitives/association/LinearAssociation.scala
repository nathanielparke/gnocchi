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
package org.bdgenomics.gnocchi.primitives.association

import breeze.linalg.{ DenseMatrix, DenseVector }

case class LinearAssociation(xTx: Array[Double],
                             xTy: Array[Double],
                             residualDegreesOfFreedom: Int,
                             weights: List[Double],
                             numSamples: Int,
                             numPredictors: Int,
                             geneticParameterStandardError: Option[Double] = None,
                             tStatistic: Option[Double] = None,
                             pValue: Option[Double] = None) extends Association

object LinearAssociation {
  def apply(xTx: DenseMatrix[Double],
            xTy: DenseVector[Double],
            residualDegreesOfFreedom: Int,
            weights: List[Double],
            numSamples: Int,
            numPredictors: Int,
            geneticParameterStandardError: Double,
            tStatistic: Double,
            pValue: Double) = new LinearAssociation(xTx.toArray, xTy.toArray, residualDegreesOfFreedom, weights, numSamples, numPredictors, Some(geneticParameterStandardError), Some(tStatistic), Some(pValue))
}