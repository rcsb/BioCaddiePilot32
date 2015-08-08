package org.biocaddie.datamention.mine;



import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.biocaddie.datamention.download.TarFileReader;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class PdbDataMentionMiner
{
	private static final int NUM_THREADS = 4;
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 25000;

	public static void main(String args[]) throws Exception
	{
        boolean firstBatch = true;
		long start = System.nanoTime();
		
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		sc.getConf().registerKryoClasses(new Class[]{DataMentionRecord.class});
		SQLContext sql = SparkUtils.getSqlContext(sc);

		int m = args.length-1;
		System.out.println(Arrays.toString(args));
		System.out.println("Number of input files: " + m);

		String outdir = args[args.length-1];

		int count = 0;

		for (int i = 0; i < m; i++) {
			System.out.println("Processing: " + args[i]);

			TarFileReader reader = new TarFileReader(args[i], BATCH_SIZE);
			
			while (reader.hasNext()) {
				List<Tuple2<String, byte[]>> tuples = reader.getNext();
				count += tuples.size();
				JavaPairRDD<String, byte[]> data = sc.parallelizePairs(tuples, NUM_THREADS*NUM_TASKS);
				JavaRDD<DataMentionRecord> records = data.flatMap(new PdbDataMentionMapper());
				// Apply a schema to an RDD of JavaBeans and register it as a table.
				DataFrame dataRecords = sql.createDataFrame(records, DataMentionRecord.class);
				
				if (firstBatch) {
					System.out.println("new records");
					dataRecords.write().mode(SaveMode.Overwrite).parquet(outdir);
					firstBatch = false;
				} else {
					System.out.println("appending records");
					dataRecords.write().mode(SaveMode.Append).parquet(outdir);
				}
			}
		}
		
		sc.close();
		
		System.out.println("All results: " + count);
		
		long time = System.nanoTime() - start;

		System.out.println("time: " + time/1E9 + " s");
	}
}
