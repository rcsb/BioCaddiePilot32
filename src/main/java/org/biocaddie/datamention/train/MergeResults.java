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
		
		// merge all results and select relevant columns
		DataFrame union = positivesI.unionAll(positivesII).unionAll(predicted).coalesce(NUM_PARTITIONS).cache();	
		union = union.select("pdb_id","match_type","deposition_year","pmc_id","pm_id","publication_year","primary_citation", "sentence");
		union = union.sort("pdb_id", "pmc_id");
		
		// save detailed PDB data mentions
		String detailsParquetFileName = workingDirectory + "/PdbDataMentionDetails.parquet";
		union.write().format("parquet").mode(SaveMode.Overwrite).save(detailsParquetFileName);
		
		String detailsTsvFileName = workingDirectory + "/PdbDataMentionDetails.tsv";
		DataFrameToDelimitedFileWriter.writeTsv(detailsTsvFileName, union);
		
		// save unique PDB ID, PMC ID data mention pairs
		DataFrame unique = union.drop("match_type").drop("sentence").distinct().sort("pdb_id", "pmc_id");
		
		String uniqueParquetFileName = workingDirectory + "/PdbDataMentionUnique.parquet";
		unique.write().format("parquet").mode(SaveMode.Overwrite).save(uniqueParquetFileName);
		
		String uniqueTsvFileName = workingDirectory + "/PdbDataMentionUnique.tsv";
		DataFrameToDelimitedFileWriter.writeTsv(uniqueTsvFileName, unique);

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}
}
