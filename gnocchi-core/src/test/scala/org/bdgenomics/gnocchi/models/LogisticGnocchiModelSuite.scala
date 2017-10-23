package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.mockito.Mockito

import scala.collection.mutable

class LogisticGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("Unit test of LGM.mergeVariantModels") {
    // (TODO) To unit test mergeVariantModels requires a seperate constructor that takes a mock of Dataset[LogisticVariantModel] which is fairly messy
  }

  sparkTest("LogisticGnocchiModel correctly combines GnocchiModels") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val observations = new Array[(Double, Double)](3)
    observations(0) = (10.0, 8.04)
    observations(1) = (8.0, 6.95)
    observations(2) = (13.0, 7.58)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val LogisticGnocchiModel = LogisticGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val observationsSecond = new Array[(Double, Double)](3)
    observationsSecond(0) = (23.0, 4.04)
    observationsSecond(1) = (29.0, 3.95)
    observationsSecond(2) = (32.0, 2.58)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val LogisticGnocchiModelSecond = LogisticGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedModel = LogisticGnocchiModel.mergeGnocchiModel(LogisticGnocchiModelSecond)
    assert(mergedModel.metaData.numSamples == 2)

    // mergeGnocchiModel::mergeVariantModels tested in LogisticVariantModelSuite
    // mergeGnocchiModel::mergeQCVariants tested below
  }

  sparkTest("Functional test of LGM.mergeVariantModels") {
    // (TODO) NOTE: SHOULD BE TESTED IN Logistic_VARIANT_MODEL_SUITE
  }

  sparkTest("LogisticGnocchiModel.mergeQCVariants correct combines variant samples") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    // Create First LogisticGnocchiModel
    val observations = new Array[(Double, Double)](3)
    observations(0) = (10.0, 8.04)
    observations(1) = (8.0, 6.95)
    observations(2) = (13.0, 7.58)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val LogisticGnocchiModel = LogisticGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Create Second LogisticGnocchiModel
    val observationsSecond = new Array[(Double, Double)](3)
    observationsSecond(0) = (23.0, 4.04)
    observationsSecond(1) = (29.0, 3.95)
    observationsSecond(2) = (32.0, 2.58)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val LogisticGnocchiModelSecond = LogisticGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedQCVariants = LogisticGnocchiModel.mergeQCVariants(LogisticGnocchiModelSecond.QCVariantModels)
    val verifyQCVariants = genotypeStates ++ genotypeStatesSecond

    assert(verifyQCVariants.toSet == mergedQCVariants.map(_.samples).collect.flatten.toSet)
  }
}
