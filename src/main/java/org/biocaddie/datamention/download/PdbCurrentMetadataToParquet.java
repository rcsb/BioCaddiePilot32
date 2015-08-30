package org.biocaddie.datamention.download;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class retrieves metadata for released and unreleased PDB entries using RCSB PDB web services
 * and saves it as a Spark DataFrame in Parquet format
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html).
 * PDB entries are updated every Wednesday around 00:00 UCT. To get the latest data rerun this application
 * every Wednesday.
 * 
 * @author Peter Rose
 */

public class PdbCurrentMetadataToParquet {
	private static String OUTPUT_FILE_NAME = "PdbCurrentMetaData.parquet";
	private static String OUTPUT_FORMAT = "parquet";
	
	private static final String CURRENT_URL = "http://www.rcsb.org/pdb/rest/customReport.csv?pdbids=*&customReportColumns=pmc,pubmedId,depositionDate&service=wsfile&format=csv&primaryOnly=1";
	private static final String UNRELEASED_URL = "http://www.rcsb.org/pdb/rest/getUnreleased";
	private static final int NUM_PARTITIONS = 4;

	public static void main(String[] args) {
		String outputDirectory = args[0];
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		
		PdbCurrentMetadataToParquet downloader = new PdbCurrentMetadataToParquet();
		downloader.writeMetadata(outputFileName, OUTPUT_FORMAT);
	}
	
	/**
	 * Parses PDB metadata and writes results as a Spark DataFrame.
	 * @param outputFileName output file name
	 * @param outputFormat output format
	 */
	public void writeMetadata(String outputFileName, String outputFormat) {
		// setup Spark and Spark SQL
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		// register custom class with Kryo serializer for best performance
		sc.getConf().registerKryoClasses(new Class[]{PdbMetaData.class});

		// download metadata
		List<PdbMetaData> entries = downloadReleased();
		entries.addAll(downloadUnreleased());

		// convert list to a distributed data object
		JavaRDD<PdbMetaData> rdd = sc.parallelize(entries, NUM_PARTITIONS);

		// Convert RDD to a DataFrame using a Java Bean as the schema definition
		DataFrame metadata = sqlContext.createDataFrame(rdd, PdbMetaData.class);
		
	    // standardize column names
		metadata = SparkUtils.toRcsbConvention(metadata);
		
		// show schema and some sample data
		metadata.printSchema();
		metadata.show();
		
		// save DataFrame
//		metadata.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(fileName);
	    metadata.write().format(outputFormat).mode(SaveMode.Overwrite).save(outputFileName);

		sc.close();
		
		System.out.println(entries.size() + " PDB metadata records saved to: " + outputFileName);
	}

    /**
     * Returns a list of metadata for released PDB entries
     * @return list of metadata objects
     */
	private List<PdbMetaData> downloadReleased() {
		List<PdbMetaData> citations = new ArrayList<>();

		try {
			URL u = new URL(CURRENT_URL);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(u.openStream()));

			String line = null;

			while ((line = reader.readLine()) != null) {
				String[] row = line.split(",");
				if (row.length != 4) {
					System.out.println("Incomplete: " + line);
				}
				if (! row[0].startsWith("structureId")) {
					String pdbId = removeQuotes(row[0]);
					String pmcId = removeQuotes(row[1]);
					if (pmcId.isEmpty()) {
						pmcId = null;
					}
					String pmId = removeQuotes(row[2]);
					if (pmId.isEmpty()) {
						pmId = null;
					}
					String date = removeQuotes(row[3]);
					if (date.length() < 4) {
						System.err.println("WARNING: Skiping invalid deposition date in: " + date);
						continue;
					}
					Integer depositionYear = null;
					try {
						depositionYear = Integer.valueOf(date.substring(0,4));
					} catch (NumberFormatException en) {
						System.err.println("WARNING: Skiping invalid deposition date in: " + date);
						continue;
					}
					Date depositionDate = Date.valueOf(date);

					PdbMetaData citation = new PdbMetaData(pdbId, pmcId, pmId, depositionYear, depositionDate, PdbMetaData.CURRENT);

					citations.add(citation);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return citations;
		}
		return citations;
	}

    /**
     * Returns a list of metadata for unreleased PDB entries
     * @return list of metadata objects
     */
	private List<PdbMetaData> downloadUnreleased() {
		List<PdbMetaData> citations = new ArrayList<>();

		try {
			URL u = new URL(UNRELEASED_URL);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(u.openStream()));

			String line = null;
			String pdbId = null;
			String date = null;

			while ((line = reader.readLine()) != null) {
				int beginIndex = line.indexOf("structureId=\"");
				if (beginIndex > 0) {
					pdbId = line.substring(beginIndex + 13, beginIndex + 17);
				}
				beginIndex = line.indexOf("initialDepositionDate=\"");
				if (beginIndex > 0) {
					date = line.substring(beginIndex + 23, beginIndex + 33);
					Integer depositionYear = null;
					Date depositionDate = null;
					try {
						depositionDate = Date.valueOf(date);
						depositionYear = Integer.valueOf(line.substring(beginIndex + 23, beginIndex + 27));
					} catch (Exception e) {
						System.out.println("Unreleased PDB " + pdbId + ": unparsable date: " + date);
					}

					PdbMetaData citation = new PdbMetaData(pdbId, null, null, depositionYear, depositionDate, PdbMetaData.UNRELEASED);
					citations.add(citation);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return citations;
		}
		return citations;
	}

	private static String removeQuotes(String string) {
		return string.replaceAll("\"", "");
	}
}
