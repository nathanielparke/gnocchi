package org.bdgenomics.gnocchi.models

import org.apache.spark
import org.apache.spark.sql.{SparkSession, Dataset}
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.{QualityControlVariantModel, LinearVariantModel}
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.mockito.Mockito


class LinearGnocchiModelSuite extends GnocchiFunSuite {
  val lgm = Mockito.mock(classOf[LinearGnocchiModel])

  sparkTest("Unit test of LGM.mergeGnocchiModel") {
    val mockModel = Mockito.mock(classOf[GnocchiModel[LinearVariantModel, LinearGnocchiModel]])

    val mockMetadata = Mockito.mock(classOf[GnocchiModelMetaData])
    val mockVariantModels = SparkSession.builder().getOrCreate().emptyDataset[LinearVariantModel]; //Mockito.mock(classOf[Dataset[LinearVariantModel]])
    val mockQCReturns = Mockito.mock(classOf[Dataset[CalledVariant]])
    val mockQCModels = Mockito.mock(classOf[Dataset[QualityControlVariantModel[LinearVariantModel]]])
    val mockQCPhenotypes = Mockito.mock(classOf[Map[String, Phenotype]])

    val model = new LinearGnocchiModel(mockMetadata, mockVariantModels, mockQCModels, mockQCPhenotypes)
    model.mergeVariantModels(mockVariantModels)

    Mockito.verify(mockVariantModels).joinWith(mockVariantModels, mockVariantModels("uniqueID") === mockVariantModels("uniqueID"))
    //Mockito.when(model.mergeGnocchiModel(mockModel).mergeVariantModels(mockModel.variantModels)).thenReturn(mockVariantModels)

//    Mockito.when(model.mergeVariantModels(mockModel.variantModels)).thenReturn(mockVariantModels)
//    Mockito.when(model.mergeQCVariants(mockQCModels)).thenReturn(mockQCReturns)

//    model.mergeGnocchiModel(mockModel)
//    Mockito.verify(model).mergeVariantModels(mockModel.variantModels)
  }

  test("Unit test of LGM.mergeVariantModels") {}

  test("Unit test of LGM.mergeQCVariants") {}

  test("Functional test of LGM.mergeGnocchiModel") {}

  test("Functional test of LGM.mergeVariantModels") {}

  test("Functional test of LGM.mergeQCVariants") {}
}
