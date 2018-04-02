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
package org.bdgenomics.gnocchi.sql

import java.io.{ ObjectInputStream, Serializable }

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{ array, col, lit, udf }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Column, Dataset, SparkSession }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.{ GenotypeRDD, VariantContextRDD, VariantRDD }
import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.gnocchi.models.{ GnocchiModel, LinearGnocchiModel, LogisticGnocchiModel }
import org.bdgenomics.gnocchi.models.variant.{ LinearVariantModel, LogisticVariantModel, VariantModel }
import org.bdgenomics.gnocchi.primitives.association.{ Association, LinearAssociationBuilder }
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.ModelType._
import org.bdgenomics.utils.misc.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

object GnocchiSession {

  /**
   * Implicitly convert a [[SparkContext]] object to a [[GnocchiSession]] object
   *
   * @param sc existing [[SparkContext]]
   * @return [[GnocchiSession]] which includes much of the core functions
   */
  implicit def sparkContextToGnocchiSession(sc: SparkContext): GnocchiSession =
    new GnocchiSession(sc)

  /**
   * Creates a GnocchiSession from SparkSession. Sets the active session to the
   * input session.
   *
   *
   * @param ss SparkSession
   * @return GnocchiSession
   */
  def GnocchiSessionFromSession(ss: SparkSession): GnocchiSession = {
    SparkSession.setActiveSession(ss)
    new GnocchiSession(ss.sparkContext)
  }

}

/**
 * The GnocchiSession provides functions on top of a SparkContext for loading
 * and analyzing genome data.
 *
 * @param sc The SparkContext to wrap.
 */
class GnocchiSession(@transient val sc: SparkContext)
    extends Serializable with Logging {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  /**
   * Returns a filtered Dataset of CalledVariant objects, where all values with
   * fewer samples than the mind threshold are filtered out.
   *
   * @param genotypes The Dataset of CalledVariant objects to filter on
   * @param mind The percentage threshold of samples to have filled in; values
   *             with fewer samples will be removed in this operation.
   * @param ploidy The number of sets of chromosomes
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterSamples(genotypes: Dataset[CalledVariant],
                    mind: Double,
                    ploidy: Double): Dataset[CalledVariant] = {

    require(mind >= 0.0 && mind <= 1.0,
      "`mind` value must be between 0.0 to 1.0 inclusive.")

    val x = genotypes.rdd.flatMap(
      f => {
        f.samples.map(
          g => { (g.sampleID, g.misses.toInt) })
      })
    val summed = x.reduceByKey(_ + _)

    val count = genotypes.count()
    val samplesWithMissingness =
      summed.map {
        case (a, b) => (a, b / (ploidy * count))
      }

    val keepers =
      samplesWithMissingness
        .filter(x => x._2 <= mind)
        .map(x => x._1).collect

    createCalledVariant(genotypes,
      f => f.filter(g => keepers.contains(g.sampleID)))
  }

  def filterSamples(genotypes: GenotypeDataset,
                    mind: Double,
                    ploidy: Double): GenotypeDataset = {
    val newGenotypes = filterSamples(genotypes.genotypes, mind, ploidy)
    GenotypeDataset(
      newGenotypes,
      genotypes.datasetUID,
      genotypes.allelicAssumption,
      genotypes.sampleUIDs)
  }

  /**
   * Construct a [[CalledVariant]] [[Dataset]] from another [[CalledVariant]]
   * [[Dataset]] through transforming sample data with a supplied function.
   *
   * @param genotypes the original [[CalledVariant]] [[Dataset]] that will be
   *                  transformed
   * @param samplesFn the transform function for genotypic information
   * @return a transformed [[Dataset]] of [[CalledVariant]] objects
   */
  def createCalledVariant(genotypes: Dataset[CalledVariant],
                          samplesFn: List[GenotypeState] => List[GenotypeState]): Dataset[CalledVariant] = {
    genotypes.map(f => {
      CalledVariant(f.uniqueID,
        f.chromosome,
        f.position,
        f.referenceAllele,
        f.alternateAllele,
        samplesFn(f.samples))
    })
  }

  /**
   * Returns a filtered [[Dataset]] of [[CalledVariant]] objects, where all
   * variants with values less than the specified geno or maf threshold are
   * filtered out.
   *
   * @param genotypes The [[Dataset]] of [[CalledVariant]] objects to filter
   * @param geno Fractional threshold for missingness in each genotype, where if
   *             the missingness fraction is larger than this threshold the
   *             variant will be filtered out of the association
   * @param maf Fractional threshold for Minor Allele Frequency, where if the
   *            MAF for a variant, or (1 - MAF for a variant) is less than this
   *            threshold the sample will be filtered out
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterVariants(genotypes: Dataset[CalledVariant],
                     geno: Double,
                     maf: Double): Dataset[CalledVariant] = {
    require(maf >= 0.0 && maf <= 1.0,
      "`maf` value must be between 0.0 to 1.0 inclusive.")
    require(geno >= 0.0 && geno <= 1.0,
      "`geno` value must be between 0.0 to 1.0 inclusive.")
    genotypes.filter(x => x.maf >= maf && 1 - x.maf >= maf && x.geno <= geno)
  }

  def filterVariants(genotypes: GenotypeDataset,
                     geno: Double,
                     maf: Double): GenotypeDataset = {
    val newGenotypes = filterVariants(genotypes.genotypes, geno, maf)
    GenotypeDataset(
      newGenotypes,
      genotypes.datasetUID,
      genotypes.allelicAssumption,
      genotypes.sampleUIDs)
  }

  /**
   * Returns a modified Dataset of CalledVariant objects, where any value with a
   * maf > 0.5 is recoded. The recoding is done by flipping the referenceAllele
   * and alternateAllele when the frequency of alt is greater than that of ref.
   *
   * @param genotypes The Dataset of CalledVariant objects to recode
   *
   * @return Returns an updated Dataset that has been recoded
   */
  def recodeMajorAllele(genotypes: Dataset[CalledVariant]): Dataset[CalledVariant] = {
    genotypes.map(f => {
      if (f.maf > 0.5) {
        CalledVariant(f.uniqueID,
          f.chromosome,
          f.position,
          f.alternateAllele,
          f.referenceAllele,
          f.samples.map(geno =>
            GenotypeState(geno.sampleID,
              geno.alts,
              geno.refs,
              geno.misses)))
      } else {
        f
      }
    })
  }

  def recodeMajorAllele(genotypes: GenotypeDataset): GenotypeDataset = {
    val newGenotypes = recodeMajorAllele(genotypes.genotypes)
    GenotypeDataset(newGenotypes, genotypes.datasetUID, genotypes.allelicAssumption, genotypes.sampleUIDs)
  }

  /**
   * @param pathName The path name to match.
   * @return Returns true if the path name matches a VCF format file extension.
   */
  def isVcfExt(pathName: String): Boolean = {
    pathName.endsWith(".vcf") ||
      pathName.endsWith(".vcf.gz") ||
      pathName.endsWith(".vcf.bgz")
  }

  /**
   * @note currently this does not enforce that the uniqueID is unique across the dataset. Checking uniqueness
   *       would require a shuffle, which adds overhead that might not be necessary right now.
   *
   * @param genotypesPath A string specifying the location in the file system of the genotypes file to load in.
   * @return a [[Dataset]] of [[CalledVariant]] objects loaded from a vcf file
   */
  def loadGenotypes(genotypesPath: String,
                    datasetUID: String,
                    allelicAssumption: String,
                    adamFormat: Boolean = false): GenotypeDataset = {
    val genoFile = new Path(genotypesPath)
    val fs = genoFile.getFileSystem(sc.hadoopConfiguration)
    require(List("ADDITIVE", "DOMINANT", "RECESSIVE").contains(allelicAssumption.toUpperCase),
      s"Allelic assumption ${allelicAssumption} not supported! Choose one of: ADDITIVE / DOMINANT / RECESSIVE")
    require(fs.exists(genoFile), s"Specified genotypes file path does not exist: $genotypesPath")

    if (datasetUID == "") logWarning("datasetUID is null. This is dangerous if you plan on merging models!")

    if (adamFormat) {
      loadAdamGenotypeRDD(genotypesPath, datasetUID, allelicAssumption)
    } else if (isVcfExt(genotypesPath)) {
      val vcRdd = sc.loadVcf(genotypesPath)
      val sampleIDs = vcRdd.samples.map(_.getSampleId).toSet
      val data = loadCalledVariantDSFromVariantContextRDD(vcRdd)
      GenotypeDataset(data.cache(), datasetUID, allelicAssumption, sampleIDs)
    } else {
      val genotypeDataset = loadGnocchiGenotypes(genotypesPath)

      require(genotypeDataset.datasetUID != datasetUID, s"Passed datasetUID `$datasetUID` does not equal the saved model's UID `${genotypeDataset.datasetUID}")
      require(genotypeDataset.allelicAssumption != allelicAssumption, s"Passed datasetUID `$allelicAssumption` does not equal the saved model's UID `${genotypeDataset.allelicAssumption}")
      genotypeDataset
    }
  }

  def loadGnocchiGenotypes(genotypesPath: String): GenotypeDataset = {
    val genoFile = new Path(genotypesPath)
    val fs = genoFile.getFileSystem(sc.hadoopConfiguration)
    require(fs.exists(genoFile), s"Specified genotypes file path does not exist: $genotypesPath")

    // todo: how should we deal with conflict between parameters and what is in the serialized
    // todo: model? What happens if the passed datasetUID is different than the serialized UID?
    val metaDataPath = new Path(genotypesPath + "/metaData")

    val path_fs = metaDataPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val ois = new ObjectInputStream(path_fs.open(metaDataPath))
    val metaData = ois.readObject.asInstanceOf[GenotypeDataset]
    ois.close

    val data = if (genotypesPath.split(",").length > 2) {
      sparkSession.read.parquet(genotypesPath.split(",").map(_ + "/genotypes"): _*).as[CalledVariant]
    } else {
      sparkSession.read.parquet(genotypesPath + "/genotypes").as[CalledVariant]
    }

    GenotypeDataset(data, metaData.datasetUID, metaData.allelicAssumption, metaData.sampleUIDs)
  }

  private def loadAdamGenotypeRDD(genotypesPath: String,
                                  datasetUID: String,
                                  allelicAssumption: String): GenotypeDataset = {
    val genotypesRDD: GenotypeRDD = sc.loadParquetGenotypes(genotypesPath)
    val variantContextRDD: VariantContextRDD = genotypesRDD.toVariantContexts()
    val sampleIDs = variantContextRDD.samples.map(_.getSampleId).toSet
    val data = loadCalledVariantDSFromVariantContextRDD(variantContextRDD)
    GenotypeDataset(data.cache(), datasetUID, allelicAssumption, sampleIDs)
  }

  private def loadCalledVariantDSFromVariantContextRDD(vcRDD: VariantContextRDD): Dataset[CalledVariant] = {
    vcRDD.rdd.map(vc => {
      val variant = vc.variant.variant
      val contigName = if (variant.getContigName.toLowerCase() == "x") 23 else variant.getContigName.toInt
      val rs_id = if (variant.getNames.size > 0) variant.getNames.get(0) else variant.getContigName + "_" + variant.getEnd.toString

      val genotypeStates = vc.genotypes.map(geno => {
        GenotypeState(geno.getSampleId,
          geno.getAlleles.count(_ == GenotypeAllele.REF).toByte,
          geno.getAlleles.count(al => al == GenotypeAllele.ALT || al == GenotypeAllele.OTHER_ALT).toByte,
          geno.getAlleles.count(_ == GenotypeAllele.NO_CALL).toByte)
      }).toList

      CalledVariant(
        rs_id,
        contigName,
        variant.getEnd.intValue(),
        variant.getReferenceAllele,
        variant.getAlternateAllele,
        genotypeStates)
    }).toDS()
  }

  /**
   * Returns a map of phenotype name to phenotype object, which is loaded from
   * a file, specified by phenotypesPath
   *
   * @todo Eventually this should be reimplemented without spark. The dataframe abstraction is nice, but there are
   *      limitations. Data manipulations we want to support. We want a pandas like object
   *      - automatic detection of missing values
   *      - converting categorical data into dummy variables
   *      - indexed columns that can be accessed by phenotype name
   *      - phenotypic summary information like histograms for particular phenotypes
   *
   * solution = use java file input stream
   *
   * @param phenotypesPath A string specifying the location in the file system
   *                       of the phenotypes file to load in.
   * @param primaryID The primary sample ID
   * @param phenoName The primary phenotype
   * @param delimiter The delimiter used in the input file
   * @param covarPath Optional parameter specifying the location in the file
   *                  system of the covariants file
   * @param covarNames Optional paramter specifying the names of the covariants
   *                   detailed in the covariants file
   * @param covarDelimiter The delimiter used in the covariants file
   *
   * @return A Map of phenotype name to Phenotype object
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String, // try to remove this
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None,
                     covarDelimiter: String = "\t",
                     missing: List[String] = List("-9")): PhenotypesContainer = {

    val phenoFile = new Path(phenotypesPath)
    val fs = phenoFile.getFileSystem(sc.hadoopConfiguration)
    require(fs.exists(phenoFile), s"Specified phenotypes file path does not exits: ${phenotypesPath}")
    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    // ToDo: keeps these operations on one machine, because phenotypes are small.
    val prelimPhenotypesDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(phenotypesPath)

    val phenoHeader = prelimPhenotypesDF.schema.fields.map(_.name)

    require(phenoHeader.length > 1,
      s"The specified delimiter '$delimiter' does not separate fields in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(phenoName),
      s"The primary phenotype, '$phenoName' does not exist in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(primaryID),
      s"The primary sample ID, '$primaryID' does not exist in the specified file, '$phenotypesPath'")

    val phenotypesDF = prelimPhenotypesDF
      .select(primaryID, phenoName)
      .toDF("sampleId", "phenotype_stage")
      .filter(!$"phenotype_stage".isin(missing: _*))
      .withColumn("phenotype", $"phenotype_stage".cast("double"))
      .drop($"phenotype_stage")

    val covariateDF = if (covarPath.isDefined && covarNames.isDefined) {
      val prelimCovarDF = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", covarDelimiter)
        .load(covarPath.get)

      val covarHeader = prelimCovarDF.schema.fields.map(_.name)

      require(covarHeader.length > 1,
        s"The specified delimiter '$covarDelimiter' does not separate fields in the specified file, '${covarPath.get}'")
      require(covarNames.get.forall(covarHeader.contains(_)),
        s"One of the covariates, '%s' does not exist in the specified file, '%s'".format(covarNames.get.toString(), covarPath.get))
      require(covarHeader.contains(primaryID),
        s"The primary sample ID, '$primaryID' does not exist in the specified file, '%s'".format(covarPath.get))
      require(!covarNames.get.contains(phenoName),
        s"The primary phenotype, '$phenoName' cannot be listed as a covariate. '%s'".format(covarNames.get.toString()))

      Option(prelimCovarDF
        .select(primaryID, covarNames.get: _*)
        .toDF("sampleId" :: covarNames.get: _*))
    } else {
      require(covarPath.isEmpty && covarNames.isEmpty, "Covariate path needs to be specified with covariate names.")
      None
    }

    val phenoCovarDF = if (covariateDF.isDefined) {
      val joinedDF = phenotypesDF.join(covariateDF.get, Seq("sampleId"))

      val filterCov = udf { (tags: mutable.WrappedArray[String]) => tags.forall(x => !missing.contains(x)) }
      val castCov = udf { (tags: mutable.WrappedArray[String]) => tags.map(x => x.toDouble) }

      joinedDF
        .withColumn("covariates_staged", array(covarNames.get.head, covarNames.get.tail: _*))
        .filter(filterCov($"covariates_staged"))
        .withColumn("covariates", castCov($"covariates_staged"))
        .select("sampleId", "phenotype", "covariates")
    } else {
      phenotypesDF.withColumn("covariates", array())
    }

    // need to filter out the missing covariates as well.
    val phenotypes = phenoCovarDF
      .withColumn("phenoName", lit(phenoName))
      .as[Phenotype]
      .collect()
      .map(x => (x.sampleId, x)).toMap

    PhenotypesContainer(sc.broadcast(phenotypes), phenoName, covarNames)
  }

  /**
   * shamelessly lifted from here:
   * https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql
   *
   * @param schema
   * @param prefix
   * @return
   */
  def flattenSchema(schema: StructType,
                    prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _              => Array(col(colName))
      }
    })
  }

  def saveAssociations[A <: Association](associations: Dataset[A],
                                         outPath: String,
                                         forceSave: Boolean,
                                         saveAsText: Boolean = false) = {
    // save dataset
    val associationsFile = new Path(outPath)
    val fs = associationsFile.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(associationsFile)) {
      if (forceSave) {
        fs.delete(associationsFile, true)
      } else {
        val input = scala.io.StdIn.readLine(s"Specified output file ${outPath} already exists. Overwrite? (y/n)> ")
        if (input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes")) {
          fs.delete(associationsFile, true)
        }
      }
    }

    val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")
    val necessaryFields = List("uniqueID", "chromosome", "position", "pValue", "genotypeStandardError").map(col)
    //    val fields = flattenSchema(associations.schema).filterNot(necessaryFields.contains(_)).toList

    val assoc = associations
      .select(necessaryFields: _*).sort($"pValue".asc)

    // enables saving as parquet or human readable text files
    if (saveAsText) {
      assoc.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(outPath)
    } else {
      associations.write.parquet(outPath)
    }
  }

  implicit def stringToModelType(input: String): ModelType = {
    if (input.toUpperCase() == "LOGISTIC") {
      Logistic
    } else if (input.toUpperCase() == "LINEAR") {
      Linear
    } else {
      throw new IllegalArgumentException(s"Unable to convert $input to a model type.")
    }
  }

  def loadGnocchiModel(modelPath: String,
                       modelType: ModelType) = {

    val modelFile = new Path(modelPath)
    val fs = modelFile.getFileSystem(sc.hadoopConfiguration)
    require(fs.exists(modelFile), s"Specified genotypes file path does not exist: $modelPath")

    // todo: how should we deal with conflict between parameters and what is in the serialized
    // todo: model? What happens if the passed datasetUID is different than the serialized UID?
    val metaDataPath = new Path(modelPath + "/metaData")

    val path_fs = metaDataPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val ois = new ObjectInputStream(path_fs.open(metaDataPath))
    // below is a hack so that we can read the metadata separately to determine what type of model it is.
    val metaData = ois.readObject.asInstanceOf[LinearGnocchiModel]
    ois.close()

    if (metaData.modelType == Linear) {
      val data = sparkSession.read.parquet(modelPath + "/variantModels").as[LinearVariantModel]
      LinearGnocchiModel(data,
        metaData.phenotypeNames,
        metaData.covariatesNames,
        metaData.sampleUIDs,
        metaData.numSamples,
        metaData.allelicAssumption)
    } else {
      val data = sparkSession.read.parquet(modelPath + "/variantModels").as[LogisticVariantModel]
      LogisticGnocchiModel(data,
        metaData.phenotypeNames,
        metaData.covariatesNames,
        metaData.sampleUIDs,
        metaData.numSamples,
        metaData.allelicAssumption)
    }
  }

  //    /**
  //     * see https://stackoverflow.com/questions/16386252/scala-deserialization-class-not-found for the object input stream
  //     * fix on qcPhenotypes
  //     *
  //     * @param path
  //     * @return
  //     */
  //    def loadGnocchiModel(path: String): GnocchiModel[_, _] = {
  //      val metaDataPath = new Path(path + "/metaData")
  //
  //      val path_fs = metaDataPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
  //      val ois = new ObjectInputStream(path_fs.open(metaDataPath))
  //      val metaData = ois.readObject.asInstanceOf[GnocchiModelMetaData]
  //      ois.close
  //
  //      val qcPhenotypesPath = new Path(path + "/qcPhenotypes")
  //      val qcPhenotypes_fs = qcPhenotypesPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
  //      val ois_2 = new ObjectInputStream(qcPhenotypes_fs.open(qcPhenotypesPath)) {
  //        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
  //          try { Class.forName(desc.getName, false, getClass.getClassLoader) }
  //          catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
  //        }
  //      }
  //
  //      val qcPhenotypes = ois_2.readObject.asInstanceOf[Map[String, Phenotype]]
  //      ois_2.close
  //
  //      if (metaData.modelType == "LinearRegression") {
  //        val variantModels = sparkSession.read.parquet(path + "/variantModels").as[LinearVariantModel]
  //        val qcVariantModels = sparkSession.read.parquet(path + "/qcModels").as[QualityControlVariantModel[LinearVariantModel]]
  //
  //        LinearGnocchiModel(metaData, variantModels, qcVariantModels, qcPhenotypes)
  //      } else {
  //        val variantModels = sparkSession.read.parquet(path + "/variantModels").as[LogisticVariantModel]
  //        val qcVariantModels = sparkSession.read.parquet(path + "/qcModels").as[QualityControlVariantModel[LogisticVariantModel]]
  //
  //        LogisticGnocchiModel(metaData, variantModels, qcVariantModels, qcPhenotypes)
  //      }
  //    }
}
