package org.biocaddie.datamention.analysis;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.biocaddie.datamention.download.PmcTarBallReader;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class PcmFindSimilarPapers
{
	private static final int NUM_THREADS = 4;
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 100;
	
	private String pmcFileName;
	private String outputFileName;
	private Date lastUpdated;
	private String[] pmcFileList;
	
	private DataFrame pmcFileMetaData;
	private Set<String> uniqueFileNames = new HashSet<>();
	private Map<String, Date> updateMap = new HashMap<>();
	
	private JavaSparkContext sparkContext;
	private SQLContext sqlContext;
	
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();
		
		PcmFindSimilarPapers miner = new PcmFindSimilarPapers();
		
		miner.parseCommandLine(args);
		
		miner.initializeSpark();
		
		miner.loadPmcFileMetaData();
		
		miner.createUpdateMap();
		
		miner.mine();
		
		miner.closeSpark();
		
		long time = System.nanoTime() - start;

		System.out.println("time: " + time/1E9 + " s");
	}
	
	private void parseCommandLine(String[] args) {
		
		if (args.length < 3) {
			System.out.println("Usage: RcsbPdbDataMentionMiner pmcFileList.parquet pmcFileNames ...  outputFile.parquet [-lastUpdated date]");
            System.out.println(" latUpdated: ");
            throw new IllegalArgumentException("Invalid command line arguments");
		}
		
		setPmcFileName(args[0]);
		
		if (args[args.length-2].equals("-lastUpdated")) {
			setLastUpdated(Date.valueOf(args[args.length-1]));
			setOutputFileName(args[args.length-3]);
	        setPmcFileList(Arrays.copyOfRange(args, 1, args.length-3));
		} else {
			setLastUpdated(Date.valueOf("1972-01-01"));
			setOutputFileName(args[args.length-1]);
	        setPmcFileList(Arrays.copyOfRange(args, 1, args.length-1));
		}
		
		System.out.println("Last updated    : " + getLastUpdated());
		System.out.println("PMC File List   : " + getPmcFileName());
		System.out.println("PMC File Names  : " + Arrays.toString(getPmcFileList()));
		System.out.println("Output File Name: " + getOutputFileName());
	}
	
	public String getPmcFileName() {
		return pmcFileName;
	}

	public void setPmcFileName(String pmcFileName) {
		this.pmcFileName = pmcFileName;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String[] getPmcFileList() {
		return pmcFileList;
	}

	public void setPmcFileList(String[] pmcFileList) {
		this.pmcFileList = pmcFileList;
	}

	public void setSparkContext(JavaSparkContext javaSparkContext) {
		this.sparkContext = javaSparkContext;
	}

	public void setSqlContext(SQLContext sqlContext) {
		this.sqlContext = sqlContext;
	}
		
	public void mine() throws Exception
	{
		Set<String> uniqueFileNames = new HashSet<>();
		
        boolean firstBatch = true;

		int count = 0;

		for (String fileName: pmcFileList) {
			System.out.println("Processing: " + fileName);

			PmcTarBallReader reader = new PmcTarBallReader(fileName, BATCH_SIZE);
			reader.setUpdateMap(updateMap);
			reader.setLastUpdated(lastUpdated);
			
			while (reader.hasNext()) {
				List<Tuple2<String, byte[]>> tuples = reader.getNext();
				
				tuples = trimRedundantRecords(tuples);
				count += tuples.size();
				
				JavaPairRDD<String, byte[]> data = sparkContext.parallelizePairs(tuples, NUM_THREADS*NUM_TASKS);
				
				JavaPairRDD<String, Vector> tf = data.mapValues(new PmcArticleHasher());
//				tf.foreach(v -> System.out.println(v));
				
				
				
				List<Vector> v = tf.values().collect();
				System.out.println("v size: " + v.size());
				List<String> f = tf.keys().collect();
				RowMatrix m = new RowMatrix(tf.values().rdd());
				CoordinateMatrix s = m.columnSimilarities(0.0);
				JavaRDD<MatrixEntry> entries = s.entries().toJavaRDD().filter(e -> e.value() > 0.9);
				System.out.println("columnsimilarities: " + entries.count());
//				entries.foreach(e -> System.out.println(e));
//				List<MatrixEntry> list = entries.collect();
//			
//				for (MatrixEntry e: list) {
//					System.out.println(e.i() + " - " + e.j());
//					double[] vi = v.get((int)e.i()).toArray();
//					double[] vj = v.get((int)e.j()).toArray();
//					double li = Math.sqrt(dot(vi, vi));
//					double lj = Math.sqrt(dot(vj, vj));
//					double cos = dot(vi, vj)/ (li * lj);
//					System.out.println("cos: " + cos + ": " + e.value());
//				}

	//			System.out.println(columnSimilarities.toString());
				int ct = 0;
				for (int i = 0; i < v.size()-1; i++) {;
					double[] vi = v.get(i).toArray();
					double li = Math.sqrt(dot(vi, vi));
					for (int j = i + 1; j < v.size(); j++) {
						double[] vj = v.get(j).toArray();
						double lj = Math.sqrt(dot(vj, vj));
	                    double cos = dot(vi, vj)/ (li * lj);

	                    if (cos > 0.9) {
						System.out.println(f.get(i) + " - " + f.get(j) + ": " + cos);
						ct++;
	                    }
					}
				}
				System.out.println("Cos-similarity: " + ct);
//				JavaRDD<DataMentionRecord> records = data
//				.filter(new PdbRegExFilter())
//				.flatMap(new PdbDataMentionMapper());
				
				// Apply a schema to an RDD of JavaBeans and register it as a table.
//				DataFrame dataRecords = sqlContext.createDataFrame(records, DataMentionRecord.class);
				
				if (firstBatch) {
					System.out.println("new records");
//					dataRecords.write().mode(SaveMode.Overwrite).parquet(getOutputFileName());
					firstBatch = false;
				} else {
					System.out.println("appending records");
//					dataRecords.write().mode(SaveMode.Append).parquet(getOutputFileName());
				}
			}
		}
		
		System.out.println("Processed records: " + count);
	}
	
	private double dot(double[] a, double[] b) {
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			sum += a[i] * b[i];
		}
		return sum;
	}
	
	private List<Tuple2<String, byte[]>> trimRedundantRecords(List<Tuple2<String, byte[]>> tuples) {
		System.out.println("Original size: " + tuples.size());
		for (Iterator<Tuple2<String, byte[]>> iter = tuples.iterator(); iter.hasNext();) {
			Tuple2<String, byte[]>  tuple = iter.next();
			if (uniqueFileNames.contains(tuple._1)) {
				iter.remove();
//				System.out.println("removing: " + tuple._1);
			} else {
				uniqueFileNames.add(tuple._1);
			}
		}
		System.out.println("Unique new entries: " + tuples.size());
		return tuples;
	}
	
	private void createUpdateMap() {
		Row[] rows = pmcFileMetaData.select("fileName","updateDate").collect();
		
		for (Row r: rows) {
			updateMap.put(r.getString(0), r.getDate(1));
		}	
	}
	
	private void loadPmcFileMetaData() {
		// pmcFileMetadata: pmcId, pmId, fileName, lastUpdated
		pmcFileMetaData = sqlContext.read().parquet(pmcFileName);
	}
	
	private void initializeSpark() {
		sparkContext = SparkUtils.getJavaSparkContext();
//		sparkContext.getConf().registerKryoClasses(new Class[]{DataMentionRecord.class});
		sqlContext = SparkUtils.getSqlContext(sparkContext);
	}
	
	private void closeSpark() {
		sparkContext.close();
	}
}
