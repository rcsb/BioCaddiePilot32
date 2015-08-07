package org.biocaddie.DataConverters;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class PmcFileListToParquet {
	private static final int NUM_THREADS = 1;
	private static final SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy MMM dd");
	private static final SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
    	PmcFileListToParquet ptp = new PmcFileListToParquet();
    	ptp.writeToParquet(args[0], args[1]);
    }
	
	public void writeToParquet(String pmcFileListName, String parquetFileName) {
		JavaSparkContext sc = getSparkContext();
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		
		List<PmcFileEntry> entries = readPmcFileEntries(pmcFileListName);
		JavaRDD<PmcFileEntry> rdd = sc.parallelize(entries);
		
		// Apply a schema to an RDD of JavaBeans
		DataFrame dataRecords = sqlContext.createDataFrame(rdd, PmcFileEntry.class);
		dataRecords.write().mode(SaveMode.Overwrite).parquet(parquetFileName);
		
		System.out.println(entries.size() + " PMC File records saved to: " + parquetFileName);
	}

	public static class PmcFileEntry implements Serializable {
		private static final long serialVersionUID = 1L;
		private String fileName;
		private String citation;
		private String pmcId;
		private String pmId;
		private Integer publicationYear;
		private Date publicationDate;
		private Date updateDate;
		
		public PmcFileEntry(String fileName, String citation, String pmcId, String pmId, Integer publicationYear, Date publicationDate, Date updateDate) {
			this.fileName = fileName;
			this.citation = citation;
			this.pmcId = pmcId;
			this.pmId = pmId;
			this.publicationYear = publicationYear;
			this.publicationDate = publicationDate;
			this.updateDate = updateDate;		
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		
		public String getCitation() {
			return citation;
		}

		public void setCitation(String citation) {
			this.citation = citation;
		}

		public String getPmcId() {
			return pmcId;
		}

		public void setPmcId(String pmcId) {
			this.pmcId = pmcId;
		}

		public String getPmId() {
			return pmId;
		}

		public void setPmId(String pmId) {
			this.pmId = pmId;
		}

		public Integer getPublicationYear() {
			return publicationYear;
		}

		public void setPublicationYear(Integer publicationYear) {
			this.publicationYear = publicationYear;
		}

		public Date getPublicationDate() {
			return publicationDate;
		}

		public void setPublicationDate(Date publicationDate) {
			this.publicationDate = publicationDate;
		}

		public Date getUpdateDate() {
			return updateDate;
		}

		public void setUpdateDate(Date updateDate) {
			this.updateDate = updateDate;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("FileName: ");
			sb.append(fileName);
		    sb.append(", PMC Id: ");
			sb.append(pmcId);
		    sb.append(", PM Id: ");
			sb.append(pmId);
		    sb.append(", PublicationYear: ");
			sb.append(publicationYear);
		    sb.append(", PublicationDate: ");
			sb.append(publicationDate);
		    sb.append(", UpdateDate: ");
			sb.append(updateDate);
			return sb.toString();
		}
	}
	
	public List<PmcFileEntry> readPmcFileEntries(String fileName) {
		List<PmcFileEntry> entries = new ArrayList<>();

		int incomplete = 0;

		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));

			String line = null;

			while ((line = reader.readLine()) != null) {
				if (line.startsWith("File")) {
					continue;
				}
				String[] row = line.split(",");
				if (row.length == 5) {
					Date publicationDate = getPublicationDate(row[1]);
					Integer publicationYear = getPublicationYear(row[1]);
					Date updateDate = getUpdateDate(row[3]);

					PmcFileEntry entry = new PmcFileEntry(trimFilePath(row[0]), row[1], row[2], row[4], 
							publicationYear, publicationDate, updateDate);
					entries.add(entry);
					//					System.out.println(entry);
				} else if (row.length == 4) {
					// PubMed Id is missing, set to null
						Date publicationDate = getPublicationDate(row[1]);
						Integer publicationYear = getPublicationYear(row[1]);
						Date updateDate = getUpdateDate(row[3]);

						PmcFileEntry entry = new PmcFileEntry(trimFilePath(row[0]), row[1], row[2], null, 
								publicationYear, publicationDate, updateDate);
						entries.add(entry);
						//					System.out.println(entry);
				} else {
					System.out.println("incomplete: " + line);
					incomplete++;
				}
				//			entries.add(entry);
			}
			reader.close();
			

		} catch (Exception e) {
			e.printStackTrace();
			return entries;
		}
		
		System.out.println("WARNING: " + incomplete + " entries skipped");
		
		return entries;
	}
	
	private Integer getPublicationYear(String citation) {
		int beginIndex = citation.indexOf('.') + 1;
		int endIndex = citation.indexOf(';');
		
		Integer publicationYear = null;
		if (beginIndex > 0 && endIndex > beginIndex) {
			String dateString = citation.substring(beginIndex, endIndex).trim().substring(0, 4);
			try {
			   publicationYear = Integer.valueOf(dateString);
			} catch (NumberFormatException e) {
				System.err.println("Unparsable publication year: " + dateString);
			}
		}
		
		return publicationYear;
	}

	private Date getPublicationDate(String citation) {
		Date publicationDate = null;
		int beginIndex = citation.indexOf('.') + 1;
		int endIndex = citation.indexOf(';');
		
		if (beginIndex > 0 && endIndex > beginIndex) {
			String dateString = citation.substring(beginIndex, endIndex).trim();
			try {
				String date = dateFormat2.format(dateFormat1.parse(dateString));
				publicationDate = Date.valueOf(date);
			} catch (ParseException e) {
				System.err.println("Unparsable date: " + citation);
				//			e.printStackTrace();
			}
		}

		return publicationDate;
	}

	private static String trimFilePath(String fileName) {
		int index = fileName.lastIndexOf("/");
		return fileName.substring(index+1,fileName.length()-7);
	}
	
	private Date getUpdateDate(String dateString)
	{
		int index = dateString.indexOf(" ");
		if (index > 0) {
	       return Date.valueOf(dateString.substring(0, index));
		} else {
			return null;
		}
	}
	
	private static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf()
				.setMaster("local[" + NUM_THREADS + "]")
				.setAppName(PmcFileListToParquet.class.getSimpleName());

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		return sc;
	}
}
