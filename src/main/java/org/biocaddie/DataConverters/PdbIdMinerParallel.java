package org.biocaddie.DataConverters;



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

import scala.Tuple2;

public class PdbIdMinerParallel
{
	private static final int NUM_THREADS = 4;
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 25000;

	public static void main(String args[]) throws Exception
	{
        boolean firstBatch = true;
		long start = System.nanoTime();
		JavaSparkContext sc = getSparkContext();
		SQLContext sql = getSqlContext(sc);

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
				JavaRDD<DataMentionRecord> records = data.flatMap(new PdbMentionMapper());
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

//		Class[] classes = {DataMentionRecord.class};
//		conf.registerKryoClasses(classes);
		conf.registerKryoClasses(new Class[]{DataMentionRecord.class});
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
