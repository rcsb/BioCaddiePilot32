package org.biocaddie.datamention.analysis;


import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.rcsb.spark.util.SparkUtils;

public class AnalyzeDataMentions {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		String workingDirectory = args[0];
		
		// read PDB data mentions
		String pdbMentionFileName = workingDirectory + "/PdbDataMentionFinal.parquet";
		DataFrame mentions = sqlContext.read().parquet(pdbMentionFileName).cache(); 
		System.out.println("PDB data mentions: " + mentions.count());
		DataFrame mentionsByYear = mentions.groupBy("publication_year").count().coalesce(1).cache();
		mentionsByYear = mentionsByYear.withColumnRenamed("count", "mentions");
		sqlContext.registerDataFrameAsTable(mentionsByYear, "mentionsByYear");
		mentionsByYear.printSchema();
		mentionsByYear.show(100);
		
		System.out.println("aggregate by pmc_id and groupBy publication_year");
		Map<String,String>  aggregates = new HashMap<String,String>();
		aggregates.put("count", "mean");
		mentions.rollup("publication_year","pmc_id").count().groupBy("publication_year").agg(aggregates).show(100);
				//.groupBy("publicationYear").mean("count").coalesce(1).cache();
//		mentionsPerPmcByYear = mentionsPerPmcByYear.withColumnRenamed("count", "mentions");
//		mentionsPerPmcByYear.show(100);

		DataFrame uniqueMentions = mentions.dropDuplicates(new String[]{"pmc_id"}).drop("pdb_id").coalesce(1);
		System.out.println("Unique PDB data mentions: " + uniqueMentions.count());
		DataFrame uniqueMentionsByYear = uniqueMentions.groupBy("publication_year").count().coalesce(1).cache();
		uniqueMentionsByYear = uniqueMentionsByYear.withColumnRenamed("count", "mentions");
		sqlContext.registerDataFrameAsTable(uniqueMentionsByYear, "uniqueMentionsByYear");
		uniqueMentionsByYear.printSchema();
		uniqueMentionsByYear.show(100);
	
		//read PMC data: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
		DataFrame pmc = sqlContext.read().parquet(args[1]);
		System.out.println("PMC publications: " + pmc.count());
		DataFrame pmcByYear = pmc.groupBy("publication_year").count().coalesce(1).cache();
		pmcByYear = pmcByYear.withColumnRenamed("count", "publications");
		pmcByYear.printSchema();
		sqlContext.registerDataFrameAsTable(pmcByYear, "pmcByYear");
		pmcByYear.show(150);
		
		DataFrame cache = sqlContext.sql("SELECT p.*, m.mentions FROM pmcByYear p INNER JOIN uniqueMentionsByYear m ON p.publication_year=m.publication_year").cache();
        cache.show(200);
        cache.select(cache.col("publication_year"), cache.col("publications"), cache.col("mentions"), cache.col("mentions").divide(cache.col("publications"))).show(50);		
		
		sc.stop();
	}
}
