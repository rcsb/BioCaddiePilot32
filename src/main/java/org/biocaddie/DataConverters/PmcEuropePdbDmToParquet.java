package org.biocaddie.DataConverters;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;


public class PmcEuropePdbDmToParquet {

	public static void main(String[] args) throws IOException {

		String parquetFileName = args[args.length-1];
		
		List<PmcId> pmcIds = new ArrayList<>();
		
		for (int i = 0; i < args.length-2; i++) {
			String fileName = args[i];
			BufferedReader reader = new BufferedReader(new FileReader(fileName));

			String line = null;

			while ((line = reader.readLine()) != null) {
				String[] fields = line.split(",");
				// PMC Id is second field, unless first field is empty
                if (fields.length == 2) {
                	pmcIds.add(new PmcId(fields[1].substring(6)));
                } else {
                	pmcIds.add(new PmcId(fields[0].substring(6)));
                }
			}
			reader.close();
		}
		saveAsParquetFile(pmcIds, parquetFileName);
	}
	
	public static class PmcId {
		String pmcId;
		public PmcId(String pmcId) {
			this.pmcId = pmcId;
		}		
		public String getPmcId() {
			return pmcId;
		}
		public void setPmcId(String pmcId) {
			this.pmcId = pmcId;
		}	
	}
	
	private static void saveAsParquetFile(List<PmcId> pmcIds, String parquetFileName) {
		JavaSparkContext sc = getSparkContext();
		SQLContext sql = getSqlContext(sc);
		
        JavaRDD<PmcId> rdd = sc.parallelize(pmcIds);
		
		// Apply a schema to an RDD of JavaBeans
		DataFrame dataRecords = sql.createDataFrame(rdd, PmcId.class);
		
		dataRecords.write().mode(SaveMode.Overwrite).parquet(parquetFileName);
	}
	
	private static JavaSparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(PdbDataMentionTrainingSetGenerator.class.getSimpleName())
		.set("spark.driver.maxResultSize", "4g")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "1g");

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
	}
	
	private static SQLContext getSqlContext(JavaSparkContext sc) {
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		return sqlContext;
	}
}
