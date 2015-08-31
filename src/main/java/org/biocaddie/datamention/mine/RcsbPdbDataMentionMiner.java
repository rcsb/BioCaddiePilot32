package org.biocaddie.datamention.mine;

import java.io.IOException;
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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.biocaddie.datamention.download.PmcTarBallReader;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

/**
 * This class processes local copies of PubMedCentral Open Access subset articles downloaded from NCBI
 * (see http://www.ncbi.nlm.nih.gov/pmc/tools/ftp/) and extracts sentences with PDB data mentions. The
 * data mentions are saved as Apache Spark DataFrame in the Parquet file format 
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html).
 * 
 * It first processes the articles in the tar balls (articles.A-B.tar.gz, ..,
 * articles.O-Z.tar.gz), which contain articles in the .nxml format. Then it processes articles 
 * in the tar balls (articles.txt.0-9A-B.tar.gz, .., articles.txt.O-Z.tar.gz), which contain extracted
 * full text articles. It only extracts those articles that are not been found in the .nxml format files.
 * 
 * PMC OC is updated every Saturday. To get the latest data rerun this application
 * every Saturday.
 * 
 * This application can be run in two modes:
 * 
 * 1. Full update mines all articles from scratch
 * 
 *      RcsbPdbDataMentionMiner inputDirectory outputDirectory
 *      
 * 2. Incremental update mines starting from the date of the last update
 * 
 *      RcsbPdbDataMentionMiner inputDirectory outputDirectory -lastUpdated YYYY-MM-DD
 *     
 * The input directory must contain the tar balls downloaded from PubMedCentral. No other tar balls (.tar.gz 
 * extension) should be present in the input directory.
 * 
 * @author Peter Rose
 *
 */
public class RcsbPdbDataMentionMiner
{
	private static final String PMC_FILE_METADATA = "PmcFileMetadata.parquet";
	private static final String OUTPUT_FILE_NAME = "PdbDataMentions.parquet";
	private static final String OUTPUT_FORMAT = "parquet";
	
	private static final int NUM_TASKS = 3;
	private static final int BATCH_SIZE = 10000;
	
	private String pmcMetadataFileName;
	private String outputFileName;
	private String outputFormat;
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
		miner.writeDataMentions(args);
		
		long time = System.nanoTime() - start;

		System.out.println("Time: " + time/1E9 + " s");
	}
	
	public void writeDataMentions(String[] args) throws Exception {
		parseCommandLine(args);

		initializeSpark();

		if (isIncrementalUpdate()) {
			loadPmcFileMetaData();
			createUpdateMap();
		}

		mine();

		closeSpark();
	}
	
	private void parseCommandLine(String[] args) throws IOException {
		if (! (args.length == 2 || args.length == 4) ) {
			System.out.println("Usage: " +
					"RcsbPdbDataMentionMiner inputdirectory outputdirectory [-lastUpdated YYYY-MM-DD");
			throw new IllegalArgumentException("Invalid command line arguments: " + Arrays.toString(args));
		}
 
		String inputDirectory = args[0];
		List<String> inputFiles = PmcTarBallReader.getTarBallFileNames(inputDirectory);	
		setPmcFileList(inputFiles);
		
		String outputDirectory = args[1];
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		setOutputFileName(outputFileName);
		String pmcFileMetadata = outputDirectory + "/" + PMC_FILE_METADATA;
		setPmcMetadataFileName(pmcFileMetadata);
		setOutputFormat(OUTPUT_FORMAT);
		
		System.out.println("PMC File List   : " + getPmcFileName());
		System.out.println("Data Mention File Name: " + getOutputFileName());
		
		// check for incremental update since the last update date 
		if (args.length == 4 && args[2].equals("-lastUpdated")) {
			setLastUpdated(Date.valueOf(args[3]));
			System.out.println("Last updated       : " + getLastUpdated());
			System.out.println("PMC File metadata  : " + getPmcFileList());
		}	
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

	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}
	
	public boolean isIncrementalUpdate() {
		return this.lastUpdated != null;
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

			// read articles in batches to avoid out of memory errors.
			PmcTarBallReader reader = new PmcTarBallReader(fileName, BATCH_SIZE);
			
			reader.setUpdateMap(updateMap);
			reader.setLastUpdated(lastUpdated);
			
			// read batches and find data mentions
			while (reader.hasNext()) {
				List<Tuple2<String, byte[]>> tuples = reader.getNext();
				
				tuples = trimRedundantRecords(tuples);
				count += tuples.size();
				
				// create distributed dataset for parallel processing
				JavaPairRDD<String, byte[]> data = sparkContext.parallelizePairs(tuples, threads*NUM_TASKS);
				
				// keep records that contain PDB ID (match PDB regex) and create a separate
				// data mention record for every PDB ID found in an article
				JavaRDD<DataMentionRecord> records = data
				.filter(new PdbRegExFilter())
				.flatMap(new PdbDataMentionMapper());
				
				// convert RDD to a Spark DataFrame using the DataMentionRecord Java Bean as the
				// schema definition.
				DataFrame dataMentions = sqlContext.createDataFrame(records, DataMentionRecord.class);
				dataMentions = SparkUtils.toRcsbConvention(dataMentions);
				
				if (firstBatch) {
					System.out.println("new records");
					// show schema and sample data
					dataMentions.printSchema();
					dataMentions.show();
						
					dataMentions.write().format(getOutputFormat()).mode(SaveMode.Overwrite).save(getOutputFileName());
					firstBatch = false;
				} else {
					System.out.println("appending records");
					dataMentions.write().format(getOutputFormat()).mode(SaveMode.Append).save(getOutputFileName());
				}
			}
		}
		
		System.out.println("Processed records: " + count);
	}
	
/**
 * Trims redundant records (same file name without extension). This happens since many articles are available in 
 * .nxml and .txt format. We only keep the unique .txt files. These are articles that are only available in 
 * .pdf format that have been converted to .txt format.
 * 
 * @param tuples file name, article pairs
 * @return unique list of file name, article pairs
 */
	private List<Tuple2<String, byte[]>> trimRedundantRecords(List<Tuple2<String, byte[]>> tuples) {
		System.out.println("Original size: " + tuples.size());
		for (Iterator<Tuple2<String, byte[]>> iter = tuples.iterator(); iter.hasNext();) {
			Tuple2<String, byte[]>  tuple = iter.next();
			if (uniqueFileNames.contains(tuple._1)) {
				iter.remove();
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
