package org.biocaddie.datamention.download;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class converts a PubMedCentral file with article metadata
 * (ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz)
 * to a .parquet file.
 * 
 * @author Peter Rose
 *
 */
public class PmcArticleMetadataToParquet {

	public static void main(String[] args) throws IOException {
		PmcArticleMetadataToParquet.downloadCsv(args[0]);
	}
	
	private static void downloadCsv(String fileName) throws IOException {
		int index = fileName.lastIndexOf(".csv");
		if (index < 1) {
			throw new IOException("Invalid .csv file name: " + fileName);
		}
		
		String outputFileName = fileName.substring(0,index) + ".parquet";
	    System.out.println(fileName + " -> " + outputFileName);
		
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
	    SQLContext sql = SparkUtils.getSqlContext(sc);
	    
	    // for options see https://github.com/databricks/spark-csv
	    DataFrame df = sql.read()
	    		.format("com.databricks.spark.csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(fileName);
	    
	    df = standardizeColumnNames(df);
	    
	    df.select("journal", "publicationYear", "pmcId")
	    .write().format("parquet")
	    .mode(SaveMode.Overwrite)
	    .save(outputFileName);
	    
	//    System.out.println("pub years: " +df.select("publicationYear").filter("publicationYear IS NOT null").count());
	//    df.printSchema();
	//    df.show(5);
	}
	
	private static DataFrame standardizeColumnNames(DataFrame df) {
	    df = df.withColumnRenamed("Journal Title", "journal");
	    df = df.withColumnRenamed("Year", "publicationYear");
	    df = df.withColumnRenamed("Volume", "volume");
	    df = df.withColumnRenamed("Issue", "issue");
	    df = df.withColumnRenamed("Page", "page");
	    df = df.withColumnRenamed("DOI", "doi");
	    df = df.withColumnRenamed("PMCID", "pmcId");
	    df = df.withColumnRenamed("PMID", "pmId");
	    df = df.withColumnRenamed("Manuscript Id", "manuscriptId");
	    df = df.withColumnRenamed("Release Date", "releaseDate");
	    
		System.out.println("columns: " + Arrays.toString(df.columns()));
		return df;
	}
}
