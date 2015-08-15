package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;


public class MergeResults {
	private static final int NUM_PARTITIONS = 4;

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
//		SparkContext sc = getSparkContext();
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		System.out.println(Arrays.toString(args));
		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		DataFrame set0 = sqlContext.read().parquet(args[0]).cache(); 
		DataFrame subset0 = set0.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count0: " + subset0.count());
		
		DataFrame set1 = sqlContext.read().parquet(args[1]).cache(); 
		DataFrame subset1 = set1.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count1: " + subset1.count());
		
		DataFrame set2 = sqlContext.read().parquet(args[2]).cache(); 
		DataFrame subset2 = set2.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count2: " + subset2.count());
		
		DataFrame set3 = sqlContext.read().parquet(args[3]).cache(); 
		DataFrame subset3 = set3.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count1: " + subset3.count());
		
		DataFrame set4 = sqlContext.read().parquet(args[4]).cache(); 
		DataFrame subset4 = set4.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count4: " + subset4.count());
		
		DataFrame set5 = sqlContext.read().parquet(args[5]).cache(); 
		DataFrame subset5 = set5.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
		System.out.println("count5: " + subset5.count());
		
		
		long start = System.nanoTime();
		
		DataFrame union = subset0.unionAll(subset1).unionAll(subset2).unionAll(subset3).unionAll(subset4).unionAll(subset5).coalesce(NUM_PARTITIONS).cache();
//		union.groupBy("depositionYear").count().show(100);
		union.filter("publicationYear IS NOT null").groupBy("publicationYear").count().show(100);
//		union.groupBy("depositionYear").mean("publicationYear").show(100);
//		union.groupBy("depositionYear").min("publicationYear").show(100);//
//		union.groupBy("depositionYear").max("publicationYear").show(100);
		
//		union.groupBy("pdbId", "pmcId").count().show(100);
		DataFrame unique = union.distinct().cache();
		DataFrame counts = unique.groupBy("pmcId").count().cache();
//		counts.sort("count").show(100);
		counts.sort("count").groupBy("count").count().show(100);

	//	union.groupBy("depositionYear").agg(aggregates).show(100);
		System.out.println("Unique pdbIds: " + unique.select("pdbId").distinct().count());
		System.out.println("Unique pmcIds: " + unique.select("pmcId").distinct().count());
		
		System.out.println("Distinct mentions: " + unique.count());
		
		 try {
			 DataFrameToDelimitedFileWriter.write(args[6],  "\t", unique);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		unique.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[7]);
		
	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}
}
