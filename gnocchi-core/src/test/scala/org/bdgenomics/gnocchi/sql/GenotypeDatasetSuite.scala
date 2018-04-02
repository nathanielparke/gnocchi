package org.bdgenomics.gnocchi.sql

import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.bdgenomics.gnocchi.sql.GnocchiSession._

class GenotypeDatasetSuite extends GnocchiFunSuite {
  sparkTest("GenotypeDataset.save should save a GenotypeDataset object for future use.") {
    val testGenos = testFile("time_genos_1.vcf")
    val genotypes = sc.loadGenotypes(testGenos, "", "ADDITIVE")

    val tempFile = tmpFile("out/")

    try {
      genotypes.save(tempFile)
    } catch {
      case e: Throwable => { print(e); fail("Error on save gnocchi formatted genotypes") }
    }

    val loadedGenotypes = try {
      sc.loadGnocchiGenotypes(tempFile)
    } catch {
      case e: Throwable => { print(e); fail("Error on load gnocchi formatted genotypes") }
    }

    assert(loadedGenotypes.allelicAssumption == genotypes.allelicAssumption, "Allelic assumption changed!")
    assert(loadedGenotypes.datasetUID == genotypes.datasetUID, "The Dataset Unique ID changed!")
    assert(loadedGenotypes.sampleUIDs == genotypes.sampleUIDs, "the sampleUIDs changed!")
  }

  sparkTest("GenotypeDataset.transformAllelicAssumption should change the allelic assumption and nothing else.") {
    val testGenos = testFile("time_genos_1.vcf")
    val genotypes = sc.loadGenotypes(testGenos, "", "ADDITIVE")

    val recodedGenotypes = genotypes.transformAllelicAssumption("DOMINANT")

    assert(recodedGenotypes.allelicAssumption == "DOMINANT", "Changing allelic assumption fails")
    assert(recodedGenotypes.datasetUID == genotypes.datasetUID, "The Dataset Unique ID changed!")
    assert(recodedGenotypes.sampleUIDs == genotypes.sampleUIDs, "the sampleUIDs changed!")
  }
}
