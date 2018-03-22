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
package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel

case class LogisticGnocchiModel(variantModels: Dataset[LogisticVariantModel],
                                phenotypeNames: String,
                                covariatesNames: List[String],
                                sampleUIDs: Set[String],
                                numSamples: Int,
                                allelicAssumption: String)
    extends GnocchiModel[LogisticVariantModel, LogisticGnocchiModel] {

  /**
   * Saves Gnocchi model by saving GnocchiModelMetaData as Java object,
   * variantModels as parquet, and comparisonVariantModels as parquet.
   */
  def save(saveTo: String): Unit = {
    //    variantModels.write.parquet(saveTo + "/variantModels")
    //    QCVariantModels.write.parquet(saveTo + "/qcModels")
    //
    //    val qcPhenoPath = new Path(saveTo + "/qcPhenotypes")
    //    val metaDataPath = new Path(saveTo + "/metaData")
    //
    //    val path_fs = qcPhenoPath.getFileSystem(variantModels.sparkSession.sparkContext.hadoopConfiguration)
    //    val path_oos = new ObjectOutputStream(path_fs.create(qcPhenoPath))
    //
    //    path_oos.writeObject(QCPhenotypes)
    //    path_oos.close
    //
    //    val metaData_fs = metaDataPath.getFileSystem(variantModels.sparkSession.sparkContext.hadoopConfiguration)
    //    val metaData_oos = new ObjectOutputStream(metaData_fs.create(metaDataPath))
    //
    //    metaData_oos.writeObject(metaData)
    //    metaData_oos.close
  }
}
