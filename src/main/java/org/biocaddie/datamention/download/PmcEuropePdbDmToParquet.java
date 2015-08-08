package org.biocaddie.datamention.download;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;


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
	
	private static void saveAsParquetFile(List<PmcId> pmcIds, String parquetFileName) {
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		SQLContext sql = SparkUtils.getSqlContext(sc);
		
        JavaRDD<PmcId> rdd = sc.parallelize(pmcIds);
		
		// Apply a schema to an RDD of JavaBeans
		DataFrame dataRecords = sql.createDataFrame(rdd, PmcId.class);
		
		dataRecords.write().mode(SaveMode.Overwrite).parquet(parquetFileName);
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
	
}
