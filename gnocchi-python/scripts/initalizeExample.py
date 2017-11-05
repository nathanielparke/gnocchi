from bdgenomics.gnocchi.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.linearGnocchiModel import LinearGnocchiModel

genotypesPath1 = "../examples/testData/time_genos_1.vcf"
phenotypesPath1 = "../examples/testData/tab_time_phenos_1.txt"

gs = GnocchiSession(spark)
genos = gs.loadGenotypes(genotypesPath1)
phenos = gs.loadPhenotypes(phenotypesPath1, "IID", "pheno_1", "\t")

lgm = LinearGnocchiModel(sc, genos, phenos, ["AD"], ["GI"])