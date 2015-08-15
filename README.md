BioCADDIE Pilot 3.2
====

# Introduction

The [BioCADDIE Pilot 3.2](https://biocaddie.org/group/pilot-project/pilot-project-3-2-development-citation-and-data-access-metrics-applied-rcsb/) is data mining platform to cross-link data and publications that is part of the larger [BioCADDIE project](https://biocaddie.org/). This prototype project provides tools for extracting data set mentions from the [Protein Data Bank](http://www.rcsb.org/) in the full text publications of the [PubMedCentral](http://www.ncbi.nlm.nih.gov/pmc/) [Open Access Subset](http://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/). It also offers tools to analyze citation networks in PubMedCentral using a number of network metrics.

This project operates on the following data sets

* Protein Data Bank: >110,000 3D structure of biomolecules. [current list](http://www.rcsb.org/pdb/results/results.do?qrid=E5798DC6&tabtoshow=Current)
* PubMedCentral Open Access Subset: >1 million free text articles



**UNDER CONSTRUCTION**

[Network Analysis](https://github.com/rcsb/BioCaddiePilot32/blob/master/src/main/java/NetworkAnalysis.md)


This project is build using [Apache Spark][Spark] and [Apache Parquet][Parquet].

## Apache Spark

[Apache Spark][Spark] allows developers to write algorithms in succinct code that can run fast locally, on an in-house cluster or on Amazon, Google or Microsoft clouds. 

## Apache Parquet

[Apache Parquet][Parquet] is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.

- Parquet compresses legacy genomic formats using standard columnar techniques (e.g. RLE, dictionary encoding). 
[Spark]: https://spark.apache.org/
[Parquet]: https://parquet.apache.org/

