from bdgenomics.gnocchi.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.linearGnocchiModel import LinearGnocchiModel
from bdgenomics.gnocchi.primitives import CalledVariantDataset, LinearVariantModelDataset, Phenotype, PhenotypeMap

gs = GnocchiSession(spark)

ss = sc
genotypes = CalledVariantDataset([], sc)
phenotypes = PhenotypeMap({}, sc, gs._jgs)
phenotypeNames = ["A", "B"]

lgm = LinearGnocchiModel(ss, genotypes, phenotypes, phenotypeNames)

