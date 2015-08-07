package org.biocaddie.DataConverters;



import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class PdbPrimaryCitationToParquet {
	private static final String CURRENT_URL = "http://www.rcsb.org/pdb/rest/customReport.csv?pdbids=*&customReportColumns=pmc,pubmedId,depositionDate&service=wsfile&format=csv&primaryOnly=1";
    private static final String UNRELEASED_URL = "http://www.rcsb.org/pdb/rest/getUnreleased";
    private static final String OBSOLETE_URL = "http://www.rcsb.org/pdb/rest/getObsolete";
    
	public static final Integer CURRENT = 1;
	public static final Integer OBSOLETE = 2;
	public static final Integer UNRELEASED = 3;
	public static final Integer UNASSIGNED = 4;
	
	private static final int NUM_THREADS = 1;
	
    public static void main(String[] args) {
	    	PdbPrimaryCitationToParquet ptp = new PdbPrimaryCitationToParquet();
	    	ptp.writeToParquet(args[0]);
 }

	public void writeToParquet(String parquetFileName) {
		JavaSparkContext sc = getSparkContext();
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		
		List<PdbPrimaryCitation> entries = downloadPrimaryCitations();
		entries.addAll(downloadUnreleased());
		entries.addAll(downloadObsolete());
		
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
					
					PdbPrimaryCitation citation = new PdbPrimaryCitation(pdbId, pmcId, pmId, depositionYear, depositionDate, CURRENT);
					
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
				
					PdbPrimaryCitation citation = new PdbPrimaryCitation(pdbId, null, null, depositionYear, depositionDate, UNRELEASED);
					citations.add(citation);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return citations;
		}
		return citations;
	}
	
	private List<PdbPrimaryCitation> downloadObsolete() {
		List<PdbPrimaryCitation> citations = new ArrayList<>();
		
		try {
			URL u = new URL(OBSOLETE_URL);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(u.openStream()));

			String line = null;

			while ((line = reader.readLine()) != null) {
				int beginIndex = line.indexOf("structureId=\"");
				if (beginIndex > 0) {
					String pdbId = line.substring(beginIndex + 13, beginIndex + 17);
					PdbPrimaryCitation citation = new PdbPrimaryCitation(pdbId, null, null, null, null, OBSOLETE);
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
	
	private static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf()
				.setMaster("local[" + NUM_THREADS + "]")
				.setAppName(PdbPrimaryCitationToParquet.class.getSimpleName());

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		return sc;
	}
	
//	public static class PrimaryCitation implements Serializable {
//		private static final long serialVersionUID = 1L;
//		private String pdbId;
//		private String pmcId;
//		private String pmId;
//		private Integer depositionYear;
//		private Date depositionDate;
//		private Integer entryType;
//		
//		public PrimaryCitation(String pdbId, String pmcId, String pmId,
//				Integer depositionYear, Date depositionDate, Integer entryType) {
//			this.pdbId = pdbId;
//			this.pmcId = pmcId;
//			this.pmId = pmId;
//			this.depositionYear = depositionYear;
//			this.depositionDate = depositionDate;
//			this.entryType = entryType;
//		}
//		
//		public String getPdbId() {
//			return pdbId;
//		}
//		public void setPdbId(String pdbId) {
//			this.pdbId = pdbId;
//		}
//		public String getPmcId() {
//			return pmcId;
//		}
//		public void setPmcId(String pmcId) {
//			this.pmcId = pmcId;
//		}
//		public String getPmId() {
//			return pmId;
//		}
//		public void setPmId(String pmId) {
//			this.pmId = pmId;
//		}
//
//		public Integer getDepositionYear() {
//			return depositionYear;
//		}
//
//		public void setDepositionYear(Integer depositionYear) {
//			this.depositionYear = depositionYear;
//		}
//
//		public Date getDepositionDate() {
//			return depositionDate;
//		}
//
//		public void setDepositionDate(Date depositionDate) {
//			this.depositionDate = depositionDate;
//		}
//
//		public Integer getEntryType() {
//			return entryType;
//		}
//
//		public void setEntryType(Integer entryType) {
//			this.entryType = entryType;
//		}
//		
//	}
}
