package org.biocaddie.datamention.download;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class retrieves metadata for PubMedCentral Open Access (PMC OC) articles
 * and saves it as a Spark DataFrame in Parquet format
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html). 
 * Article meta data are retrieved from a local copy of PMC-ids.csv.gz available from 
 * ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz.
 * PMC OC is updated every Saturday. To get the latest data rerun this application
 * every Saturday.
 * 
 * @author Peter Rose
 */
public class PmcArticleMetadataToParquet {
	private static String INPUT_FILE_NAME = "PMC-ids.csv.gz";
	private static String OUTPUT_FILE_NAME = "PmcArticleMetaData.parquet";
	private static String OUTPUT_FORMAT = "parquet";

	public static void main(String[] args) throws IOException {
		String inputDirectory = args[0];
		String outputDirectory = args[1];
		String inputFileName = inputDirectory + "/" + INPUT_FILE_NAME;
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		PmcArticleMetadataToParquet downloader = new PmcArticleMetadataToParquet();
		
		downloader.writeToParquet(inputFileName, outputFileName, OUTPUT_FORMAT);
	}
	
	/**
	 * Parses PDB metadata and writes results as a Spark DataFrame in the Parquet file format.
	 * @param fileName Parquet file
	 * @throws IOException 
	 */
	public void writeToParquet(String inputFileName, String outputFileName, String outputFormat) throws IOException {		
	    System.out.println(inputFileName + " -> " + outputFileName);
		// setup Spark and Spark SQL
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
	    SQLContext sql = SparkUtils.getSqlContext(sc);
	    
	    // for options see https://github.com/databricks/spark-csv
	    DataFrame metadata = sql.read()
	    		.format("com.databricks.spark.csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(inputFileName);
	    
	    // standardize column names and data
	    metadata = standardizeColumnNames(metadata);
		// for now we only need these columns
	    metadata = metadata.select("journal", "publication_year", "pmc_id");
		
		// show schema and some sample data
		metadata.printSchema();
		metadata.show();
	    
		// save DataFrame
	    metadata.write().format(outputFormat).mode(SaveMode.Overwrite).save(outputFileName);
	    
	    System.out.println(metadata.count() + " Pmc article metadata records saved to: " + outputFileName);
	    
	    sc.close();
	}
	
	private static DataFrame standardizeColumnNames(DataFrame df) {
	    df = df.withColumnRenamed("Journal Title", "journal");
	    df = df.withColumnRenamed("Year", "publication_year");
	    df = df.withColumnRenamed("Volume", "volume");
	    df = df.withColumnRenamed("Issue", "issue");
	    df = df.withColumnRenamed("Page", "page");
	    df = df.withColumnRenamed("DOI", "doi");
	    df = df.withColumnRenamed("PMCID", "pmc_id");
	    df = df.withColumnRenamed("PMID", "pm_id");
	    df = df.withColumnRenamed("Manuscript Id", "manuscript_id");
	    df = df.withColumnRenamed("Release Date", "release_date");
	    
		return df;
	}
}
