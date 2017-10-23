package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.mockito.Mockito

import scala.collection.mutable

class LinearGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("Unit test of LGM.mergeVariantModels") {
    // (TODO) To unit test mergeVariantModels requires a seperate constructor that takes a mock of Dataset[LinearVariantModel] which is fairly messy
  }

  sparkTest("LinearGnocchiModel correctly combines GnocchiModels") {
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

    val linearGnocchiModel = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val observationsSecond = new Array[(Double, Double)](3)
    observationsSecond(0) = (23.0, 4.04)
    observationsSecond(1) = (29.0, 3.95)
    observationsSecond(2) = (32.0, 2.58)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val linearGnocchiModelSecond = LinearGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedModel = linearGnocchiModel.mergeGnocchiModel(linearGnocchiModelSecond)
    assert(mergedModel.metaData.numSamples == 2)

    // mergeGnocchiModel::mergeVariantModels tested in LinearVariantModelSuite
    // mergeGnocchiModel::mergeQCVariants tested below
  }

  sparkTest("Functional test of LGM.mergeVariantModels") {
    // (TODO) NOTE: SHOULD BE TESTED IN LINEAR_VARIANT_MODEL_SUITE
//    val spark = SparkSession.builder().master("local").getOrCreate()
//    import spark.implicits._
//
//    val observations = new Array[(Double, Double)](3)
//    observations(0) = (10.0, 8.04)
//    observations(1) = (8.0, 6.95)
//    observations(2) = (13.0, 7.58)
//
//    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
//    val cv = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStates)
//    println("VALID SAMPLES # = " + cv.numValidSamples)
//    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()
//
//    val phenoMap = observations.map(_._2)
//      .toList
//      .zipWithIndex
//      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
//      .toMap
//
//    val linearGnocchiModel = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

//    val observationsSecond = new Array[(Double, Double)](3)
//    observationsSecond(0) = (23.0, 4.04)
//    observationsSecond(1) = (29.0, 3.95)
//    observationsSecond(2) = (32.0, 2.58)
//
//    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
//    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
//    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()
//
//    val newVariantModels = LinearSiteRegression(cvDatasetSecond, sc.broadcast(phenoMap), "Additive")
//
//    val mergedVariantModel = linearGnocchiModel.mergeVariantModels(newVariantModels).collect()(0)
//
//    val oldVariantModels = linearGnocchiModel.variantModels
//
//    oldVariantModels.map(lvm => lvm.association.numSamples).foreach(i => println("oldSamples: " + i))
//    oldVariantModels.map(lvm => lvm.association.weights).foreach(i => println("oldWeights: " + i))
//    oldVariantModels.map(lvm => lvm.association.ssDeviations).foreach(i => println("oldSSDeviations: " + i))
//    oldVariantModels.map(lvm => lvm.association.ssResiduals).foreach(i => println("oldSsResiduals: " + i))
//    oldVariantModels.map(lvm => lvm.association.residualDegreesOfFreedom).foreach(i => println("oldResidualDegreesOfFreedom: " + i))
//    oldVariantModels.map(lvm => lvm.association.tStatistic).foreach(i => println("oldTStatistic: " + i))
//    oldVariantModels.map(lvm => lvm.association.pValue).foreach(i => println("oldPvalue: " + i))
//
//    println("---")
//
//    newVariantModels.map(lvm => lvm.association.numSamples).foreach(i => println("newSamples: " + i))
//    newVariantModels.map(lvm => lvm.association.weights).foreach(i => println("newWeights: " + i))
//    newVariantModels.map(lvm => lvm.association.ssDeviations).foreach(i => println("newSSDeviations: " + i))
//    newVariantModels.map(lvm => lvm.association.ssResiduals).foreach(i => println("newSsResiduals: " + i))
//    newVariantModels.map(lvm => lvm.association.residualDegreesOfFreedom).foreach(i => println("newResidualDegreesOfFreedom: " + i))
//    newVariantModels.map(lvm => lvm.association.tStatistic).foreach(i => println("newTStatistic: " + i))
//    newVariantModels.map(lvm => lvm.association.pValue).foreach(i => println("newPvalue: " + i))
//
//
//    assert(mergedVariantModel.association.numSamples == 0) // (TODO) Change on clarification, should be (old.samples + new.samples)
//    assert(mergedVariantModel.association.weights == 0) // (TODO) Change on clarification, should be [(old.weights[0] * old.numSamples + new.weights[0] * new.numSamples) / (old.numSamples + new.numSamples), (old.weights[1] * old.numSamples + new.weights[1] * new.numSamples) / (old.numSamples + new.numSamples)]
//    assert(mergedVariantModel.association.ssDeviations == 1.1977333333)
//    assert(mergedVariantModel.association.ssResiduals == 0.8564669173)
//    assert(mergedVariantModel.association.residualDegreesOfFreedom == 0) // (TODO) Change on clarification, should be old.residualDegreesOfFreedom + new.samples
//    assert(mergedVariantModel.association.tStatistic == 0)
//    assert(mergedVariantModel.association.pValue == 0)

  }

  sparkTest("LinearGnocchiModel.mergeQCVariants correct combines variant samples") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    // Create First LinearGnocchiModel
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

    val linearGnocchiModel = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Create Second LinearGnocchiModel
    val observationsSecond = new Array[(Double, Double)](3)
    observationsSecond(0) = (23.0, 4.04)
    observationsSecond(1) = (29.0, 3.95)
    observationsSecond(2) = (32.0, 2.58)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val linearGnocchiModelSecond = LinearGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedQCVariants = linearGnocchiModel.mergeQCVariants(linearGnocchiModelSecond.QCVariantModels)
    val verifyQCVariants = genotypeStates ++ genotypeStatesSecond

    assert(verifyQCVariants.toSet == mergedQCVariants.map(_.samples).collect.flatten.toSet)
  }
}
