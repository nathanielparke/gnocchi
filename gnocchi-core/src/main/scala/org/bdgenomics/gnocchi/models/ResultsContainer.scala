package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.Dataset

case class ResultsContainer(datasets: List[String],
                            results: Dataset[ResultWrapper])
