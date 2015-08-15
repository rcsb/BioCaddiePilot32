BioCADDIE Pilot 3.2
====

![picture alt](https://github.com/rcsb/BioCaddiePilot32/blob/master/src/main/resources/biocaddie-logo.png)

# Introduction

The [BioCADDIE Pilot 3.2](https://biocaddie.org/group/pilot-project/pilot-project-3-2-development-citation-and-data-access-metrics-applied-rcsb/) is a data mining platform to cross-link data and publications that is part of the larger [BioCADDIE project](https://biocaddie.org/). This prototype project provides tools for extracting data set mentions from the [Protein Data Bank](http://www.rcsb.org/) in the full text publications of the [PubMedCentral](http://www.ncbi.nlm.nih.gov/pmc/) [Open Access Subset](http://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/). It also offers tools to analyze citation networks in PubMedCentral using a number of network metrics.

This project operates on the following data sets

* Protein Data Bank (PDB): >110,000 3D structure of biomolecules ([current list](http://www.rcsb.org/pdb/results/results.do?qrid=E5798DC6&tabtoshow=Current))
* PubMedCentral Open Access Subset: >1 million free text articles ([current list](http://www.ncbi.nlm.nih.gov/pmc/?term=open+access[filter]))

## What are Data Mentions?

Data mentions are references to data sets in publications that fall into two categories: 1. structured data mentions can be recognized by regular expressions matching, 2. unstructured data mentions require natural language processing and machine learning to disambiguate valid from invalid data mentions.

Structured data mentions for PDB Identifiers

Reference          | Example
------------------ |---------
PDB ID             | `PDB ID: 1STP`
PDB DOI            | `http://dx.doi.org/10.2210/pdb1stp/pdb`
RCSB PDB URL       | `http://www.rcsb.org/../structureId=1stp`
NXML External Link | `<ext-link .. ext-link-type=“pdb” xlink:href=“1STP”>`


Unstructured data mentions for PDB Identifiers

Type                | Example
------------------- | -------------
Valid (4AHQ)        | `The structure of the active site of the K165C enzyme (4AHQ) ...`
Invalid (2C19)      | `The polymorphisms of cytochrome P450 **2C19** (CYP2C19) gene ...`



**UNDER CONSTRUCTION**

[Citation Network Analysis](https://github.com/rcsb/BioCaddiePilot32/blob/master/src/main/java/NetworkAnalysis.md)

##Project Status

This project is in active development. Expect major refactoring of current code.

##Want to Use or Contribute to this Project?
Contact us <pwrose@ucsd.edu>

##Technology Stack
This project relies on the open-source technologies [Apache Spark][Spark] and [Apache Parquet][Parquet] to make literature data mining fast and parallelizable.

### Apache Spark

[Apache Spark][Spark] is a fast and general framework for large-scale in-memory data processing. It runs locally, on an in-house or commercial cloud environments. We use Spark DataFrames to store, filter, sort, and join data sets.

### Apache Parquet

[Apache Parquet][Parquet] is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language. We store Spark DataFrame as Parquet files for high-performance data handling.

[Spark]: https://spark.apache.org/
[Parquet]: https://parquet.apache.org/

