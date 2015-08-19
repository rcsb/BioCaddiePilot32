package org.biocaddie.datamention.mine;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.biocaddie.datamention.download.TarFileReader;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class RcsbPdbDataMentionMiner
{
	private static final int NUM_THREADS = 4;
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 25000;
	
	private String pmcFileName;
	private String outputFileName;
	private Date lastUpdated;
	private String[] pmcFileList;
	
	private DataFrame pmcFileMetaData;
	private Set<String> uniqueFileNames = new HashSet<>();
	
	private JavaSparkContext sparkContext;
	private SQLContext sqlContext;
	
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();
		
		RcsbPdbDataMentionMiner miner = new RcsbPdbDataMentionMiner();
		
		miner.parseCommandLine(args);
		
		miner.initializeSpark();
		
		miner.loadPmcFileMetaData();
		
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

			TarFileReader reader = new TarFileReader(fileName, BATCH_SIZE);
			
			while (reader.hasNext()) {
				List<Tuple2<String, byte[]>> tuples = reader.getNext();
				
				tuples = trimRedundantRecords(tuples);
				count += tuples.size();
				
//				JavaPairRDD<String, byte[]> data = sparkContext.parallelizePairs(tuples, NUM_THREADS*NUM_TASKS);
//				
//				JavaRDD<DataMentionRecord> records = data
//				.filter(new PdbRegExFilter())
//				.flatMap(new PdbDataMentionMapper());
//				
//				// Apply a schema to an RDD of JavaBeans and register it as a table.
//				DataFrame dataRecords = sqlContext.createDataFrame(records, DataMentionRecord.class);
//				
//				if (firstBatch) {
//					System.out.println("new records");
//					dataRecords.write().mode(SaveMode.Overwrite).parquet(getOutputFileName());
//					firstBatch = false;
//				} else {
//					System.out.println("appending records");
//					dataRecords.write().mode(SaveMode.Append).parquet(getOutputFileName());
//				}
			}
		}
		
		System.out.println("Processed records: " + count);
	}
	
	private List<Tuple2<String, byte[]>> trimRedundantRecords(List<Tuple2<String, byte[]>> tuples) {
		System.out.println("Original size: " + tuples.size());
		for (Iterator<Tuple2<String, byte[]>> iter = tuples.iterator(); iter.hasNext();) {
			Tuple2<String, byte[]>  tuple = iter.next();
			if (uniqueFileNames.contains(tuple._1)) {
				iter.remove();
				System.out.println("removing: " + tuple._1);
			} else {
				uniqueFileNames.add(tuple._1);
			}
		}
		System.out.println("Trimmed size: " + tuples.size());
		return tuples;
	}
	
	private void loadPmcFileMetaData() {
		// pmcFileMetadata: pmcId, pmId, fileName, lastUpdated
		pmcFileMetaData = sqlContext.read().parquet(pmcFileName);
	}
	
	private void initializeSpark() {
		sparkContext = SparkUtils.getJavaSparkContext();
		sparkContext.getConf().registerKryoClasses(new Class[]{DataMentionRecord.class});
		sqlContext = SparkUtils.getSqlContext(sparkContext);
	}
	
	private void closeSpark() {
		sparkContext.close();
	}
}
