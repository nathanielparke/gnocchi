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

package org.bdgenomics.gnocchi.api

import org.bdgenomics.gnocchi.api.java.{ GnocchiFunSuite, JavaGnocchiSession }
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession
import org.apache.spark.SparkContext
import org.mockito.Mockito
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite // (TODO) Replace with GnocchiFunSuite

class JavaGnocchiSessionSuite extends GnocchiFunSuite {
  sparkTest("Creating JavaGnocchiSession") {
    val gs = new GnocchiSession(sc)
    val jgs = new JavaGnocchiSession(gs)
  }

  sparkTest("Verify filterSamples makes correct call to GnocchiSession") {
    val gs = Mockito.mock(classOf[GnocchiSession])
    val jgs = new JavaGnocchiSession(gs)

    val mockGenotype = Mockito.mock(classOf[Dataset[CalledVariant]])
    val mockMind = 0.0
    val mockPloidy = 0.0

    jgs.filterSamples(mockGenotype, mockMind, mockPloidy)

    Mockito.verify(gs).filterSamples(mockGenotype, mockMind, mockPloidy)
  }
}