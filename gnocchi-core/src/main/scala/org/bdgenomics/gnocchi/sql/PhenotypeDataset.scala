package org.bdgenomics.gnocchi.sql

import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype

case class PhenotypeDataset(phenotypes: Map[String, Phenotype],
                            phenotypeName: String,
                            covariateName: List[String])