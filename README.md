# BioCaddiePilot32

[https://biocaddie.org/](https://biocaddie.org/)

PILOT PROJECT 3.2 - DEVELOPMENT OF CITATION AND DATA ACCESS METRICS APPLIED TO RCSB PROTEIN DATA BANK AND RELATED RESOURCES

LEADERS: PETER ROSE, PH.D. PROTEIN DATA BANK UCSD AND CHUN-NAN HSU, PH.D. DEPARTMENT OF BIOMEDICAL INFORMATICS UCSD

Collaborator: Cathy Wu, Ph.D. and Cecilia Arighi, Ph.D. (non funded)


[Network Analysis](https://github.com/rcsb/BioCaddiePilot32/blob/master/src/main/java/NetworkAnalysis.md)


This Readme file explains the usage of programs:

PART 1: DATA RETRIEVAL: We can retrieve data in two ways by using the flag (0:cite+ref cascades 1:whole_pubmed):

1.1: Starting from Pdb primary citations, we can first retrieve all citation+reference cascades then we can retrieve the summary of all retrieved pubmed ids. Order is important. <br>
$ java org.biocaddie.citationanalysis.retrievedata.RetrieveCitationFromEutils 0 PdbId_PubmedId_Jun27.csv <br>
$ java org.biocaddie.citationanalysis.retrievedata.RetrieveSummaryFromEutils 0 all_pubmed_id.txt 

1.2: We can retrieve the whole pubmed, by first retrieving the summary of all valid pubmed ids between 1 and n(26M), then retrieving the citations for all retrieved pubmed ids. Order is important. <br>
$ java org.biocaddie.citationanalysis.retrievedata.RetrieveSummaryFromEutils 1 26000000 whole_pubmed/ <br>
$ java org.biocaddie.citationanalysis.retrievedata.RetrieveCitationFromEutils 1 whole_pubmed/all_pubmed_id.txt 


PART 2: NETWORK CONSTRUCTION: We construct the network using the two retrieved XML files: all_citations.xml and all_citations_summary.xml. 

2.1: These XML files are too large (10GB and 25GB), in order to prevent memory problems, first we parse these XML files and write our needed fields into smaller (1.5GB and 2GB) txt files: all_citations.txt and all_citations_summary.txt. Order is not important. <br>
$ java org.biocaddie.citationanalysis.retrievedata.CitationSummaryResultXmlParser all_citations_summary.xml <br>
$ java org.biocaddie.citationanalysis.retrievedata.CitationAndRefLinkResultXmlParser all_citations.xml 

2.2: We construct the paper citation networks and journal citation networks using these two txt files: all_citations.txt and all_citations_summary.txt <br>
-First we can construct the initial paper and journal networks, which includes all papers we retrieved. <br>
$ java -Xmx28000m org.biocaddie.citationanalysis.network.ConstructNetwork all_citations.txt all_citations_summary.txt 

2.3: For temporal analysis, we can construct a paper&journal citation network using only the papers published between two years, for example 2012-2014. First we choose papers published between 2012 and 2014, and include the citations only between these papers. <br>
$ java org.biocaddie.citationanalysis.network.ConstructNetwork all_citations.txt all_citations_summary.txt 2012 2014 <br>
$ java org.biocaddie.citationanalysis.network.ConstructNetwork all_citations.txt all_citations_summary.txt 2009 2011 <br>
$ java org.biocaddie.citationanalysis.network.ConstructNetwork all_citations.txt all_citations_summary.txt 2006 2008

2.4: We can convert paper citation network to paper co-citation network, which is usually more connected (flag = 1). <br>
$ java org.biocaddie.citationanalysis.network.NetworkUtils 1 2014_2012_paper_citation_network.net

2.5: Clean the journal citation network by - excluding self-journal citations, - excluding four major interdisciplinary journals (Nature, Science, PNAS, PlosOne), and- excluding journals which make$receive citations less than 1 (flag = 2). <br>
$ java org.biocaddie.citationanalysis.network.NetworkUtils 2 2014_2012_journal_citation_network.net

2.6: Print network summary (global properties) of a given network  (flag = 3): <br>
$ java org.biocaddie.citationanalysis.network.NetworkUtils 3 2008_2006_journal_citation_network.net

PART 3: NETWORK METRICS: For a given network in Pajek.net format, compute three types of metrics for each node: inDegree centrality(citation count), pageRank and betweenness centrality. 

Call: java org.biocaddie.citationanalysis.network.NetworkMeasuresMetrics [flag] [network.net] [optional pageRankDampingFactor, default:0.5] <br>
flag values= 1:pageRank 2:betweenness, 12: both pageRank and betweenness <br>
$ java -Xmx28000m org.biocaddie.citationanalysis.network.NetworkMeasuresMetrics 1 2014_2012_paper_citation_network.net 0.5

PART 4: COMMUNITY DETECTION : We analyze the community structure using InfoMap. Download InfoMap from http://www.mapequation.org/code.html and install it using make. 

4.1: Run infoMap for the paper co-citation network in an undirected manner with N=5 attemps. N=5 number of attemps is important. Since infoMap algorithm is heuristic, for example if you run with N=1, most probably you cannot find optimum or best clustering. Run it with at least N=5, if you have time run it with N=10. <br>
$ ./Infomap 2014_2012_paper_citation_network_cocitation.net output_directory/ --hard-partitions --tree --bftree --map --undirected --seed 5234535 -N 5 -vvv

It will generate three kind of output files:  <br>
.tree: hierarchical community structure in human-readable format. <br>
.bftree: hierarchical community structure in binary format, for visualization <br>
.map: only top-level community structure in human readable format, but also visualizable.

InfoMap Hierarchical Network Navigator couldn't visualize this .bftree, it gives FlashPlayer timeout, since it contains 1M nodes. (http://www.mapequation.org/apps/NetworkNavigator.html) But we could visualize only the top-level community structure using .map file with some modification. <br>
.map file has three sections: *Modules, *Nodes and *Links, delete the content only under the *Nodes section, but keep the *Nodes line. So we can reduce the file size from 200MM to 2MB. Then upload it to the Map Generator. It will give some warning but ignore it, you can visualize the top-level community structure of the network. http://www.mapequation.org/apps/MapGenerator.html

In order to name the modules, open .tree file, copy the lines of top-level community 1, and extract only titles of papers within that community, by pasting into excel delimiters (|| and then "). Then paste the titles to the https://www.jasondavies.com/wordcloud/ to generate word cloud. (use Scale: n and orientations from 0 0) Repeat this process, let's say for the largest 15 modules.

4.2: For temporal analysis download a version of infoMap (conf-infoMap_dir) from http://www.tp.umu.se/~rosvall/downloads/conf-infomap_dir.tgz and install it using make. Then run conf-infoMap_dir for journal citation networks of differen years in a directed manner:

(10 = number of attemps and 100 = bootstrap resamples) it is important, for example if you run 5 5, then you most probably cannot find the significant clusters. <br>
$ ./conf-infomap 765677 2014_2012_journal_citation_network_clean.net 10 100 0.9 <br>
$ ./conf-infomap 765677 2011_2009_journal_citation_network_clean.net 10 100 0.9 <br>
$ ./conf-infomap 765677 2008_2006_journal_citation_network_clean.net 10 100 0.9 <br>
Then upload the .map files of all these three networks into MapGenerator and visualize the change in the mapping using the Alluvial Generator. http://www.mapequation.org/apps/MapGenerator.html

4.3: We can also run the infoMap on the full journal network, after cleaning the data (removing major interdisciplinar journals, excluding self-journal citations, excluding journals with citations less than n) By the way, conf-infoMap is not good only for temporal analysis, it may be better to find significant clustering of any (un)directed network than the classic infoMap.

PART 5: UTILITIES: Not all functions runnable from command-line, but

GenerateDataForCharts.java class includes code for the following purposes:  

Call: java org.biocaddie.citationanalysis.utility.GenerateDataForCharts [paperOrPdb] [pageRankorCiteCnt] [top100orOutliers] [network] [networkMetrics] [PdbIdPubMedId] <br>
[paperOrPdb]: 1:sort all papers  2: sort only PDB primary citations <br>
[pageRankorCiteCnt]: 1:use pageRank  2: use CiteCount <br>
[top100orOutliers]: 1:top100  2: outliers within top100

$ java org.biocaddie.citationanalysis.utility.GenerateDataForCharts 1 1 1 paper_citation_network.net paper_citation_network_metrics_d_0.5_.txt PdbId_PubMedId_April29.csv 

- Read the network metrics file and list the top-100 papers (according to PageRank or CiteRank) <br> 
- Read the network metrics file and list the top-100 PDBs   (according to PageRank or CiteRank) <br>
- Read the network metrics file and list the outliers within the top-100 (citeRank/pageRank greater than 5) <br>
- Read the network metrics file and generate chart data for average citeCount & pageRank per year 

DrugTargetCorrelation.java class includes code for the following purposes: <br>
- Read the PDB drug target table and sort them according to the pageRank or citeRank of the primary citation. Here is the relation: Generic Name - PDB ID - Primary Citation




