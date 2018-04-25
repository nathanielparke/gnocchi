from bdgenomics.gnocchi.utils.primitives import GenotypeDataset, PhenotypesContainer

class LinearSiteRegression(object):
    def __init__(self, genotypes, phenotypes):
        self._sc = genotypes.sc
        self._jvm = self._sc._jvm
        self.results = self._jvm.org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression(genotypes.get(), phenotypes.get())