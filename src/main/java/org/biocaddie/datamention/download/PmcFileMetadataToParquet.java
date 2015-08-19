package org.biocaddie.datamention.download;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class converts a PubMedCentral Open Access Subset file list, obtained from 
 * ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/file_list.csv
 * to a Spark DataFrame and saves it as a .parquet file. It renames columns to have consistent
 * names throughout the entire toolset.
 * 
 * @author Peter Rose
 *
 */
public class PmcFileMetadataToParquet {

	public static void main(String[] args) throws IOException {
		PmcFileMetadataToParquet.downloadCsv(args[0]);
	}
	
	/**
	 * 
	 * @param fileName
	 * @throws IOException
	 */
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
	    
	    df = addFileNameColumn(sql, df);
	    
	    df = addLastUpdatedColumn(sql, df);
	   
	    // for now we only need these columns
	    df = df.select("pmcId","pmId", "fileName", "lastUpdated");
	    
        df.printSchema();
        df.show(5);
       
	    df.write().format("parquet").mode(SaveMode.Overwrite).save(outputFileName);
	}

	private static DataFrame standardizeColumnNames(DataFrame df) {
		System.out.println("Original column names: " + Arrays.toString(df.columns()));
	    df = df.withColumnRenamed("File", "file")
	    		.withColumnRenamed("Article Citation", "articleCitation")
	    		.withColumnRenamed("Accession ID", "pmcId")
	    		.withColumnRenamed("Last Updated (YYYY-MM-DD HH:MM:SS)", "updateDate")
	    	    .withColumnRenamed("PMID", "pmId");

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
	 * @return
	 */
	private static DataFrame addFileNameColumn(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("trim", (String s) -> s.substring(6,s.lastIndexOf(".tar")), DataTypes.StringType);
        df = sql.sql("SELECT d.*, trim(d.file) as fileName FROM df as d");
		return df;
	}
	
	/**
	 * Adds a lastUpdated column to the data frame. It converts the updateDate column to a timestamp 
	 * format and and adds it as lastUpdated column to the data frame. It then delete the updateDate column.
	 * 
	 * Example:
	 * updateDate: "2013-03-19 14:51:52" -> lastUpdated: "2013-03-19"
 	 *
	 * @param sql
	 * @param df
	 * @return
	 */
	private static DataFrame addLastUpdatedColumn(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("toDate", (String s) -> Timestamp.valueOf(s), DataTypes.TimestampType);
        df = sql.sql("SELECT d.*, toDate(d.updateDate) as lastUpdated FROM df as d");
        df = df.drop("updateDate");
		return df;
	}
	
	
}
