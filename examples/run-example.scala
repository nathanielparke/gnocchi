import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression

val genos = sc.loadGenotypes("./testData/test_genos.vcf", "merged_vcf", "ADDITIVE")
val phenos = sc.loadPhenotypes("./testData/test_phenos.txt", "IID", "pheno_1", "\t", Option("./testData/merge/test_phenos.txt"), Option(List("pheno_2", "pheno_3")), "\t")
val associations = LinearSiteRegression(mergedGenos, mergedPhenos).associations

val sortedAssociations = associations.sort($"pValue".asc)
