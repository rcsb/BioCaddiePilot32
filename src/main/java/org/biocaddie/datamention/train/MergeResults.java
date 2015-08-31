package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.DataFrameToDelimitedFileWriter;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class mergers all positive and predicted positive PDB data mentions into a single file.
 * 
 * @author Peter Rose
 *
 */
public class MergeResults {
	private static final int NUM_PARTITIONS = 4;

	public static void main(String[] args) throws FileNotFoundException {
		SparkContext sc = SparkUtils.getSparkContext();
		
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		String workingDirectory = args[0];

		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		String positiveIFileName = workingDirectory + "/PositivesI.parquet";
		DataFrame positivesI = sqlContext.read().parquet(positiveIFileName).cache(); 
		System.out.println("PositivesI: " + positivesI.count());
		
		String positiveIIFileName = workingDirectory + "/PositivesII.parquet";
		DataFrame positivesII = sqlContext.read().parquet(positiveIIFileName).cache(); 
		System.out.println("PositivesII: " + positivesII.count());
		
		String predictedFileName = workingDirectory + "/PdbDataMentionPredicted.parquet";
		DataFrame predicted = sqlContext.read().parquet(predictedFileName).cache(); 
		System.out.println("Predicted: " + predicted.count());
			
		long start = System.nanoTime();
		
		String dataMentionFileName = workingDirectory + "/PdbDataMentionFinal.parquet";
		DataFrame union = positivesI.unionAll(positivesII).unionAll(predicted).coalesce(NUM_PARTITIONS).cache();
		
		union.write().mode(SaveMode.Overwrite).parquet(dataMentionFileName);
		
		String dataMentionTsvFileName = workingDirectory + "/PdbDataMentionFinal.tsv";
		DataFrameToDelimitedFileWriter.writeTsv(dataMentionTsvFileName, union);
		
		
//		union.groupBy("depositionYear").count().show(100);
//		DataFrame subset2 = set2.select("pdbId", "depositionYear", "pmcId", "pmId", "publicationYear").distinct();
//		union.filter("publicationYear IS NOT null").groupBy("publicationYear").count().show(100);
//		union.groupBy("depositionYear").mean("publicationYear").show(100);
//		union.groupBy("depositionYear").min("publicationYear").show(100);//
//		union.groupBy("depositionYear").max("publicationYear").show(100);
		
//		union.groupBy("pdbId", "pmcId").count().show(100);
//		DataFrame unique = union.distinct().cache();
//		DataFrame counts = unique.groupBy("pmcId").count().cache();
//		counts.sort("count").show(100);
//		counts.sort("count").groupBy("count").count().show(100);

	//	union.groupBy("depositionYear").agg(aggregates).show(100);
//		System.out.println("Unique pdbIds: " + unique.select("pdbId").distinct().count());
//		System.out.println("Unique pmcIds: " + unique.select("pmcId").distinct().count());
		
//		System.out.println("Distinct mentions: " + unique.count());
		
//		 try {
//			 DataFrameToDelimitedFileWriter.write(args[6],  "\t", unique);
//			} catch (FileNotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		
//		unique.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[7]);
		
	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}
}
