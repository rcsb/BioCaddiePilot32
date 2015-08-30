package org.biocaddie.datamention.download;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class parses the wwPDB FTP archive for metadata of obsoleted PDB structures and PDB models 
 * and saves it as a Spark DataFrame in Parquet file format
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html).
 * PDB entries are updated every Wednesday around 00:00 UCT. To get the latest data rerun this application
 * every Wednesday.
 * 
 * @author Peter Rose
 *
 */
public class PdbObsoleteMetadataToParquet {
	private static final String OUTPUT_FILE_NAME = "PdbObsoleteMetaData.parquet";
	private static String OUTPUT_FORMAT = "parquet";
	
	private static final String PDB_HTTP = "http://ftp.wwpdb.org/";
	private static final String SERVER = "ftp.pdbj.org";
	private static final String USER_NAME = "anonymous";
	private static final String PASSWORD = "";
	
	private static final String CURRENT_MODEL_URL = "pub/pdb/data/structures/models/current/pdb";
	private static final String OBSOLETE_MODEL_URL = "pub/pdb/data/structures/models/obsolete/pdb";
	private static final String OBSOLETE_ENTRY_URL = "pub/pdb/data/structures/obsolete/pdb";
	
    private static final DateFormat inFormat = new SimpleDateFormat("dd-MMM-yy");
    private static final DateFormat outFormat1 = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat outFormat2 = new SimpleDateFormat("yyyy");
    private static final int NUM_PARTITIONS = 4;

	public static void main(String[] args) {
		String outputDirectory = args[0];
		String outputFileName = outputDirectory + "/" + OUTPUT_FILE_NAME;
		
		PdbObsoleteMetadataToParquet downLoader = new PdbObsoleteMetadataToParquet();
		downLoader.writeMetadata(outputFileName, OUTPUT_FORMAT);
	}
	
	/**
	 * Parses PDB metadata and writes results as a Spark DataFrame.
	 * 
	 * @param outputFileName file name
	 * @param outputFormat output format
	 */
	public void writeMetadata(String outputFileName, String outputFormat) {
		// setup Spark and Spark SQL
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		// register custom class with Kryo serializer for best performance
		sc.getConf().registerKryoClasses(new Class[]{PdbMetaData.class});
		
		// download metadata
		List<PdbMetaData> entries = downloadObsoleteModels();
		entries.addAll(downloadCurrentModels());
		entries.addAll(downloadObsoleteEntries());
		
		// convert list to a distributed data object
		JavaRDD<PdbMetaData> rdd = sc.parallelize(entries, NUM_PARTITIONS);
		
		// convert RDD to a DataFrame using a Java Bean as the schema definition
		DataFrame metadata = sqlContext.createDataFrame(rdd, PdbMetaData.class);
		
	    // standardize column names
		metadata = SparkUtils.toRcsbConvention(metadata);
		
		// show schema and some sample data
		metadata.printSchema();
		metadata.show();
		
	    // save DataFrame
	    metadata.write().format(outputFormat).mode(SaveMode.Overwrite).save(outputFileName);
		
		sc.close();

		System.out.println(entries.size() + " PDB metadata records saved to: " + outputFileName);
	}
	
	/**
	 * Returns list of obsolete PDB model information records.
	 * @return obsolete PDB model information
	 */
	public List<PdbMetaData> downloadObsoleteModels() {
		List<PdbMetaData> info = new ArrayList<>();
		
		List<String> obsoleteNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, OBSOLETE_MODEL_URL);
		for (String fileName: obsoleteNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbMetaData.OBSOLETE_MODEL));
		}
		
		return info;
	}

	/**
	 * Returns list of current PDB model information records.
	 * @return current PDB model information
	 */
	public List<PdbMetaData> downloadCurrentModels() {
		List<PdbMetaData> info = new ArrayList<>();
		
		List<String> currentNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, CURRENT_MODEL_URL);
		for (String fileName: currentNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbMetaData.CURRENT_MODEL));
		}
		
		return info;
	}
	
	/**
	 * Returns metadata about obsolete PDB structures.
	 * @return obsolete PDB structure information
	 */
	public List<PdbMetaData> downloadObsoleteEntries() {
		List<PdbMetaData> info = new ArrayList<>();
		
		List<String> currentNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, OBSOLETE_ENTRY_URL);
		for (String fileName: currentNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbMetaData.OBSOLETE));
		}
		
		return info;
	}

	/**
	 * Parses basic metadata (pdbId, deposition data, deposition year) from a PDB file
	 * @param url url of PDB file
	 * @param entryType the type of PDB structure or model
	 * @return
	 */
	private static PdbMetaData getModelInfo(String url, int entryType) {
		// read header line of PDB file
		String header = getHeaderLine(url);
		System.out.println(header);
		
		PdbMetaData info = new PdbMetaData();
		info.setPdbId(header.substring(62,66).toUpperCase());
		info.setEntryType(entryType);
		
		// parse deposition date/year
		try {
			String yyyymmdd = outFormat1.format(inFormat.parse(header.substring(50,59)));
			Date depositionDate = Date.valueOf(yyyymmdd); // Note, this is a java.sql.date!
			info.setDepositionDate(depositionDate);

			String yyyy = outFormat2.format(inFormat.parse(header.substring(50,59)));
			info.setDepositionYear(Integer.parseInt(yyyy));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return info;
	}

	/**
	 * Returns the header line of a PDB file at the specified URL.
	 * @param url
	 * @return
	 */
	private static String getHeaderLine(String url) {
		String header = "";
		try {
			URL u = new URL(url);
			URLConnection connection = u.openConnection();
			connection.setConnectTimeout(60000);
			InputStream stream = connection.getInputStream();

			if (stream != null) {
				try {
					BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream)));
					String line = reader.readLine();
					if (line != null) {
						reader.close();
						return line;				
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				} 
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return header;
	}
}
