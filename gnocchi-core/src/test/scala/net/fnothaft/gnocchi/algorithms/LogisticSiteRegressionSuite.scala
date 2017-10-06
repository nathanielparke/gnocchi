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
package net.fnothaft.gnocchi.algorithms

import net.fnothaft.gnocchi.sql.GnocchiSession._
import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticRegression
import net.fnothaft.gnocchi.models.logistic.AdditiveLogisticGnocchiModel
import net.fnothaft.gnocchi.primitives.genotype.GenotypeState
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.sql.{ Dataset, SparkSession }

class LogisticSiteRegressionSuite extends GnocchiFunSuite {
  val WeightThreshold: Double = 1

  sparkTest("Test logisticRegression on binary data") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    // read in the data from binary.csv
    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
    // results can be found here: http://www.ats.ucla.edu/stat/sas/dae/logit.htm
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",")) //get rows

    // transform it into the right format
    val observations = data.map(row => {
      val geno = row(0)
      val covars = row.slice(1, 3).toList
      val pheno = row(3)
      (geno, (pheno, covars))
    }).collect()

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStates)
    val cvds = sc.parallelize(List(cv)).toDS

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1._1.toDouble, item._1._2.map(_.toDouble))))
      .toMap

    val assoc = AdditiveLogisticRegression.applyToSite(phenoMap, cv)

    val estimatedWeights = assoc.weights.toArray
    val KnownWeights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    val KnownPValue = 0.0385

    val weightItems = estimatedWeights.zip(KnownWeights).zipWithIndex
    weightItems.foreach(item => {
      val ((act, exp), idx) = item
      assert(nearby(exp, act, WeightThreshold), s"Weight $idx incorrect, expected: $exp, actual $act")
    })

    assert(nearby(KnownPValue, assoc.pValue, 0.01), s"P-Value incorrect, expected: $KnownPValue, actual ${assoc.pValue}")
  }
}
