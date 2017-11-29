# PyGnocchi

Statistical associations using the ADAM genomics analysis platform. The currently supported operations are Genome Wide Association using Linear and Logistic models with either Dominant or Additive assumptions.

In order to increase accessibility, PyGnocchi is a package that exposes most of the core functionality of Gnocchi to users in Python. Also included is a guide on how to install and get started using PyGnocchi (both in shell and Jupyter Notebook) as well a detailed explanation of the available functions and how to use them.

## Installation and Usage

<span style="color:red"> ** (TODO KUNAL) Add information on how to build Gnocchi, what scripts to run, etc ** </span>


## GnocchiSession

PyGnocchi preserves almost all of the semantic ways of interacting with the core Gnocchi, including GnocchiSession. Just as in the Scala version GnocchiSession in Python provides access to loading genotypes and phenotypes, filtering out variants, and the other expected methods documented below.

### Creating a GnocchiSession

To initialize a GnocchiSession, note that you need to pass in a SparkSession object instead of the more common SparkContext constructor.

```
gs = GnocchiSession(spark)
```

As explained in the section below on Exposing Scala, this now is able to access the same methods from the core Gnocchi library using an exposed connection to the JVM.

### Methods

The methods available in the Python GnocchiSession are articulated below but see the original Scala documentation for further detail on their purpose and arguments

- `GnocchiSession.loadGenotypes` - Loads a Dataset of CalledVariant objects from a file
- `GnocchiSession.loadPhenotypes` - Returns a map of phenotype name to phenotype object, which is loaded from a file, specified by phenotypesPath
- `GnocchiSession.filterVariants` - Returns a filtered Dataset of CalledVariant objects, where all variants with values less than the specified geno or maf threshold are filtered out
- `GnocchiSession.filterSamples` - Returns a filtered Dataset of CalledVariant objects, where all values with fewer samples than the mind threshold are filtered out
- `GnocchiSession.recodeMajorAllele` - Returns a modified Dataset of CalledVariant objects, where any value with a maf > 0.5 is recoded. The recoding is specified as flipping the referenceAllele and alternateAllele when the frequency of alt is greater than that of ref

### Examples

Load data from genotype and phenotype files
```
genos = gs.loadGenotypes(genotypesPath)
phenos = gs.loadPhenotypes(phenotypesPath, "SampleID", "pheno1", "\t")
```

Filter out variants and samples
```
filteredGenos = gs.filterSamples(genos, 0.1, 2)
filteredGenosVariants = gs.filterVariants(filteredGenos, 0.1, 0.1)
```

## GnocchiModel 

<span style="color:red"> ** (TODO KUNAL) Write section ** </span>

### Creating GnocchiModels

### Methods

### Examples


## Accessing Spark primitives

<span style="color:red"> ** (TODO ADITHYA) Write primitive summary ** </span>

### Exposing Scala

<span style="color:red"> ** (TODO ADITHYA) How Py4J exposes underlying methods ** </span>

### Dos and Donts

<span style="color:red"> ** (TODO KUNAL) Make table of functions we can and can't access via the JVM </span>

