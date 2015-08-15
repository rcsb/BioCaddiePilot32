package org.biocaddie.datamention.analysis;


import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class AnalyzeDataMentions {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		System.out.println(Arrays.toString(args));
		
		// read PDB data mentions
		DataFrame mentions = sqlContext.read().parquet(args[0]).cache(); 
		System.out.println("PDB data mentions: " + mentions.count());
		DataFrame mentionsByYear = mentions.groupBy("publicationYear").count().coalesce(1).cache();
		mentionsByYear = mentionsByYear.withColumnRenamed("count", "mentions");
		sqlContext.registerDataFrameAsTable(mentionsByYear, "mentionsByYear");
		mentionsByYear.printSchema();
		mentionsByYear.show(100);
		
		System.out.println("aggregate by pmcId and groupBy publicationYear");
		Map<String,String>  aggregates = new HashMap<String,String>();
		aggregates.put("count", "mean");
		mentions.rollup("publicationYear","pmcId").count().groupBy("publicationYear").agg(aggregates).show(100);
				//.groupBy("publicationYear").mean("count").coalesce(1).cache();
//		mentionsPerPmcByYear = mentionsPerPmcByYear.withColumnRenamed("count", "mentions");
//		mentionsPerPmcByYear.show(100);

		DataFrame uniqueMentions = mentions.dropDuplicates(new String[]{"pmcId"}).drop("pdbId").coalesce(1);
		System.out.println("Unique PDB data mentions: " + uniqueMentions.count());
		DataFrame uniqueMentionsByYear = uniqueMentions.groupBy("publicationYear").count().coalesce(1).cache();
		uniqueMentionsByYear = uniqueMentionsByYear.withColumnRenamed("count", "mentions");
		sqlContext.registerDataFrameAsTable(uniqueMentionsByYear, "uniqueMentionsByYear");
		uniqueMentionsByYear.printSchema();
		uniqueMentionsByYear.show(100);
	
		//read PMC data: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
		DataFrame pmc = sqlContext.read().parquet(args[1]);
		System.out.println("PMC publications: " + pmc.count());
		DataFrame pmcByYear = pmc.groupBy("publicationYear").count().coalesce(1).cache();
		pmcByYear = pmcByYear.withColumnRenamed("count", "publications");
		pmcByYear.printSchema();
		sqlContext.registerDataFrameAsTable(pmcByYear, "pmcByYear");
		pmcByYear.show(150);
		
		DataFrame cache = sqlContext.sql("SELECT p.*, m.mentions FROM pmcByYear p INNER JOIN uniqueMentionsByYear m ON p.publicationYear=m.publicationYear").cache();
        cache.show(200);
        cache.select(cache.col("publicationYear"), cache.col("publications"), cache.col("mentions"), cache.col("mentions").divide(cache.col("publications"))).show(50);		
		
		sc.stop();
	}
}
