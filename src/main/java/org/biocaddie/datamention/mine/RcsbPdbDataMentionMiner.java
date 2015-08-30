package org.biocaddie.datamention.mine;

import java.io.IOException;
import java.sql.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.biocaddie.datamention.download.PmcTarBallReader;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class RcsbPdbDataMentionMiner
{
	private static final String PMC_FILE_METADATA = "PmcFileMetadata.parquet";
	private static final String OUTPUT_FILE_NAME = "PdbDataMentions.parquet";
	private static final String OUTPUT_FORMAT = "parquet";
	
//	private static final int NUM_THREADS = 4;
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 20000;
	
	private String pmcMetadataFileName;
	private String outputFileName;
	private Date lastUpdated;
	private List<String> pmcFileList;
	
	private DataFrame pmcFileMetaData;
	private Set<String> uniqueFileNames = new HashSet<>();
	private Map<String, Date> updateMap = new HashMap<>();
	
	private JavaSparkContext sparkContext;
	private SQLContext sqlContext;
	
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();
		
		RcsbPdbDataMentionMiner miner = new RcsbPdbDataMentionMiner();
		
		miner.parseCommandLine(args);
		
		miner.initializeSpark();
		
		miner.loadPmcFileMetaData();
		
		miner.createUpdateMap();
		
		miner.mine();
		
		miner.closeSpark();
		
		long time = System.nanoTime() - start;

		System.out.println("time: " + time/1E9 + " s");
	}
	
	private void parseCommandLine(String[] args) throws IOException {
//		System.out.println("Usage: RcsbPdbDataMentionMiner pmcFileList.parquet pmcFileNames ...  outputFile.parquet [-lastUpdated date]");
//        System.out.println(" latUpdated: ");
//        throw new IllegalArgumentException("Invalid command line arguments");
        
		String inputDirectory = args[0];
		List<String> inputFiles = PmcTarBallReader.getTarBallFileNames(inputDirectory);	
		setPmcFileList(inputFiles);
		
		String outputDirectory = args[1];
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		setOutputFileName(outputFileName);
		String pmcFileMetadata = outputDirectory + "/" + PMC_FILE_METADATA;
		setPmcMetadataFileName(pmcFileMetadata);
		
		// check for incremental update since the last update date 
		if (args.length == 4 && args[2].equals("-lastUpdated")) {
			setLastUpdated(Date.valueOf(args[3]));
		}
		
		System.out.println("Last updated    : " + getLastUpdated());
		System.out.println("PMC File List   : " + getPmcFileName());
		System.out.println("PMC File Names  : " + getPmcFileList());
		System.out.println("Output File Name: " + getOutputFileName());
	}
	
	public String getPmcFileName() {
		return pmcMetadataFileName;
	}

	public void setPmcMetadataFileName(String pmcFileName) {
		this.pmcMetadataFileName = pmcFileName;
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

	public List<String> getPmcFileList() {
		return pmcFileList;
	}

	public void setPmcFileList(List<String> pmcFileList) {
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
        boolean firstBatch = true;
        int threads = sparkContext.defaultParallelism();
        System.out.println("Number of threads: " + threads);

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
				
				JavaPairRDD<String, byte[]> data = sparkContext.parallelizePairs(tuples, threads*NUM_TASKS);
				
				JavaRDD<DataMentionRecord> records = data
				.filter(new PdbRegExFilter())
				.flatMap(new PdbDataMentionMapper());
				
				// Apply a schema to an RDD of JavaBeans and register it as a table.
				DataFrame dataMentions = sqlContext.createDataFrame(records, DataMentionRecord.class);
				dataMentions = SparkUtils.toRcsbConvention(dataMentions);
				
				if (firstBatch) {
					System.out.println("new records");
					// show schema and sample data
					dataMentions.printSchema();
					dataMentions.show();
						
					dataMentions.write().mode(SaveMode.Overwrite).parquet(getOutputFileName());
					firstBatch = false;
				} else {
					System.out.println("appending records");
					dataMentions.write().mode(SaveMode.Append).parquet(getOutputFileName());
				}
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
//				System.out.println("removing: " + tuple._1);
			} else {
				uniqueFileNames.add(tuple._1);
			}
		}
		System.out.println("Unique new entries: " + tuples.size());
		return tuples;
	}
	
	private void createUpdateMap() {
		Row[] rows = pmcFileMetaData.select("file_name","update_date").collect();
		
		for (Row r: rows) {
			updateMap.put(r.getString(0), r.getDate(1));
		}	
	}
	
	private void loadPmcFileMetaData() {
		// pmcFileMetadata: pmcId, pmId, fileName, lastUpdated
		pmcFileMetaData = sqlContext.read().parquet(pmcMetadataFileName);
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
