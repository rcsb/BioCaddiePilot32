package org.biocaddie.MLExamples;




import java.io.FileNotFoundException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ParquetToTsv {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = getSparkContext();
		SQLContext sqlContext = getSqlContext(sc);
		
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
	
	private static SparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(ParquetToTsv.class.getSimpleName())
		.set("spark.driver.maxResultSize", "4g")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "1g");

		SparkContext sc = new SparkContext(conf);

		return sc;
	}
	
	private static SQLContext getSqlContext(SparkContext sc) {
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		return sqlContext;
	}
}
