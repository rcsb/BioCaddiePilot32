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

public class PdbPrimaryCitationToParquet {
	private static final String CURRENT_URL = "http://www.rcsb.org/pdb/rest/customReport.csv?pdbids=*&customReportColumns=pmc,pubmedId,depositionDate&service=wsfile&format=csv&primaryOnly=1";
    private static final String UNRELEASED_URL = "http://www.rcsb.org/pdb/rest/getUnreleased";
//    private static final String OBSOLETE_URL = "http://www.rcsb.org/pdb/rest/getObsolete";

	
    public static void main(String[] args) {
	    	PdbPrimaryCitationToParquet ptp = new PdbPrimaryCitationToParquet();
	    	ptp.writeToParquet(args[0]);
 }

	public void writeToParquet(String parquetFileName) {		
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		sc.getConf().registerKryoClasses(new Class[]{PdbPrimaryCitation.class});
		
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		List<PdbPrimaryCitation> entries = downloadPrimaryCitations();
		entries.addAll(downloadUnreleased());
//		entries.addAll(downloadObsolete());
		
		JavaRDD<PdbPrimaryCitation> rdd = sc.parallelize(entries);
		
		// Apply a schema to an RDD of JavaBeans
		DataFrame dataRecords = sqlContext.createDataFrame(rdd, PdbPrimaryCitation.class);
		dataRecords.write().mode(SaveMode.Overwrite).parquet(parquetFileName);
		
		System.out.println(entries.size() + " PMC File records saved to: " + parquetFileName);
	}

	
	private List<PdbPrimaryCitation> downloadPrimaryCitations() {
		List<PdbPrimaryCitation> citations = new ArrayList<>();
		
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
					String pmId = removeQuotes(row[2]);
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
					
					PdbPrimaryCitation citation = new PdbPrimaryCitation(pdbId, pmcId, pmId, depositionYear, depositionDate, PdbPrimaryCitation.CURRENT);
					
					citations.add(citation);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return citations;
		}
		return citations;
	}
	
	private List<PdbPrimaryCitation> downloadUnreleased() {
		List<PdbPrimaryCitation> citations = new ArrayList<>();
		
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
				
					PdbPrimaryCitation citation = new PdbPrimaryCitation(pdbId, null, null, depositionYear, depositionDate, PdbPrimaryCitation.UNRELEASED);
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
