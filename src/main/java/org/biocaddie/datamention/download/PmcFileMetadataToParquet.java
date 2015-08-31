package org.biocaddie.datamention.download;

import java.io.IOException;
import java.sql.Date;
import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class retrieves metadata for the PubMedCentral Open Access (PMC OC) files
 * and saves it as a Spark DataFrame in Parquet format
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html). 
 * File meta data are retrieved from a local copy of file_list.csv available from 
 * ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/file_list.csv.
 * PMC OC is updated every Saturday. To get the latest data rerun this application
 * every Saturday.
 * 
 * @author Peter Rose
 *
 */
public class PmcFileMetadataToParquet {
	private static String INPUT_FILE_NAME = "file_list.csv";
	private static String OUTPUT_FILE_NAME = "PmcFileMetadata.parquet";
	private static String OUTPUT_FORMAT = "parquet";

	public static void main(String[] args) throws IOException {
		String inputDirectory = args[0];
		String outputDirectory = args[1];
		String inputFileName = inputDirectory + "/" + INPUT_FILE_NAME;
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		
		PmcFileMetadataToParquet downloader = new PmcFileMetadataToParquet();
		downloader.writeMetadata(inputFileName, outputFileName, OUTPUT_FORMAT);
	}
	
	/**
	 * Parses PMC File metadata and writes results as a Spark DataFrame.
	 * @param outputFileName output file name
	 * @param outputFormat output format
	 */
	private void writeMetadata(String inputFileName, String outputFileName, String outputFormat) throws IOException {
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
	    metadata = addFileNameColumn(sql, metadata);	    
	    metadata = stringToDate(sql, metadata);
		metadata = SparkUtils.toRcsbConvention(metadata);
		
		// for now we only need these columns
	    metadata = metadata.select("pmc_id", "pm_id", "file_name", "update_date");
		
		// show schema and some sample data
		metadata.printSchema();
		metadata.show();
       
		// save DataFrame
	    metadata.write().format(outputFormat).mode(SaveMode.Overwrite).save(outputFileName);
	    
	    System.out.println(metadata.count() + " Pmc file metadata records saved to: " + outputFileName);
	    
	    sc.close();
	}

	private static DataFrame standardizeColumnNames(DataFrame df) {
		System.out.println("Original column names: " + Arrays.toString(df.columns()));
	    df = df.withColumnRenamed("File", "file")
	    		.withColumnRenamed("Article Citation", "article_citation")
	    		.withColumnRenamed("Accession ID", "pmc_id")
	    		.withColumnRenamed("Last Updated (YYYY-MM-DD HH:MM:SS)", "update_date")
	    	    .withColumnRenamed("PMID", "pm_id");

		System.out.println("Standardized column names: " + Arrays.toString(df.columns()));
		return df;
	}
	
	/**
	 * Adds a fileName column to the data frame. It trims the path and file extension (.tar.gz) from the 
	 * file column and adds it as the fileName column to the data frame.
	 * 
	 * Example:
	 * file: "08/e0/Breast_Cancer_Res_2001_Nov_2_3(1)_55-60.tar.gz" -> fileName: "Breast_Cancer_Res_2001_Nov_2_3(1)_55-60"
 	 *
	 * @param sql
	 * @param df
	 * @return DataFrame
	 */
	private static DataFrame addFileNameColumn(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("trim", (String s) -> s.substring(6,s.lastIndexOf(".tar")), DataTypes.StringType);
        df = sql.sql("SELECT d.*, trim(d.file) as file_name FROM df as d");
		return df;
	}
	
	/**
	 * Temporary fix: Converts the updateDate column to a Date format.
	 * (This is a missing feature in the .csv reader, I've filed a Jira ticket).
	 * 
	 * Example:
	 * "2013-03-19 14:51:52" -> "2013-03-19"
 	 *
	 * @param sql
	 * @param df
	 * @return DataFrame
	 */
	private static DataFrame stringToDate(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("toDate", (String s) -> Date.valueOf(s.substring(0,10)), DataTypes.DateType);
        df = sql.sql("SELECT d.*, toDate(d.update_date) as last_updated FROM df as d");
        df = df.drop("update_date");
        df = df.withColumnRenamed("last_updated", "update_date");
		return df;
	}
}
