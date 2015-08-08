package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.rcsb.spark.util.SparkUtils;

public class ParquetToTsv {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		long start = System.nanoTime();

		for (String fileName: args) {
			DataFrame data = sqlContext.read().parquet(fileName);
			String tsvFileName = fileName.substring(0, fileName.indexOf(".parquet")) + ".tsv";
            System.out.println(fileName + " -> " + tsvFileName);
            DataFrameToDelimitedFileWriter.write(fileName, "\t", data);
		}

		long end = System.nanoTime();
		System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}
}
