package org.biocaddie.DataConverters;



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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class PdbObsoleteInfoToParquet {
	// Obsolete files HEADER    HYDROLASE(O-GLYCOSYL)                   05-JAN-93   116L 
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

	public static void main(String[] args) {
		PdbObsoleteInfoToParquet downLoader = new PdbObsoleteInfoToParquet();
		downLoader.writeToParquet(args[0]);
	}
	
	public void writeToParquet(String parquetFileName) {
		JavaSparkContext sc = getJavaSparkContext();
		SQLContext sqlContext = getSqlContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		
		List<PdbPrimaryCitation> entries = downloadObsoleteModels();
		entries.addAll(downloadCurrentModels());
		entries.addAll(downloadObsoleteEntries());
		
		JavaRDD<PdbPrimaryCitation> rdd = sc.parallelize(entries);
		
		// Apply a schema to an RDD of JavaBeans
		DataFrame dataRecords = sqlContext.createDataFrame(rdd, PdbPrimaryCitation.class);
		dataRecords.write().mode(SaveMode.Overwrite).parquet(parquetFileName);
		
		System.out.println(entries.size() + " PMC File records saved to: " + parquetFileName);
	}
	
	public List<PdbPrimaryCitation> downloadObsoleteModels() {
		List<PdbPrimaryCitation> info = new ArrayList<>();
		
		List<String> obsoleteNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, OBSOLETE_MODEL_URL);
		for (String fileName: obsoleteNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbPrimaryCitation.OBSOLETE_MODEL));
		}
		
		return info;
	}

	public List<PdbPrimaryCitation> downloadCurrentModels() {
		List<PdbPrimaryCitation> info = new ArrayList<>();
		
		List<String> currentNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, CURRENT_MODEL_URL);
		for (String fileName: currentNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbPrimaryCitation.CURRENT_MODEL));
		}
		
		return info;
	}
	
	public List<PdbPrimaryCitation> downloadObsoleteEntries() {
		List<PdbPrimaryCitation> info = new ArrayList<>();
		
		List<String> currentNames = FtpFileLister.getFileNames(SERVER, USER_NAME, PASSWORD, OBSOLETE_ENTRY_URL);
		for (String fileName: currentNames) {
			info.add(getModelInfo(PDB_HTTP + fileName, PdbPrimaryCitation.OBSOLETE));
		}
		
		return info;
	}

	private static PdbPrimaryCitation getModelInfo(String fileName, int entryType) {
		String header = getHeaderLine(fileName);
		System.out.println(header);
		PdbPrimaryCitation info = new PdbPrimaryCitation();
		info.setPdbId(header.substring(62,66).toUpperCase());
		info.setEntryType(entryType);
		
		java.util.Date javaDate = null;
         try {
             javaDate = inFormat.parse(header.substring(50,59));
         } catch (Exception e) {
             e.printStackTrace();
         }
		String yyyymmdd = outFormat1.format(javaDate);
		System.out.println(yyyymmdd);
		Date depositionDate = Date.valueOf(yyyymmdd);
		info.setDepositionDate(depositionDate);
		
		String yyyy = outFormat2.format(javaDate);
		System.out.println(yyyy);
		info.setDepositionYear(Integer.parseInt(yyyy));

		return info;
	}

	private static String getHeaderLine(String fileName) {
		String header = "";
		try {
			URL u = new URL(fileName);
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
						//					
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					//					try {
					//						System.out.println("closing reader");
					//						reader.close();
					//						stream.close();
					//					} catch (IOException e) {
					//						e.printStackTrace();
					//					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return header;
	}

//	private static List<String> getFileNames(String url) {	
//		List<String> files = new ArrayList<>();
//		
//		FTPClient ftpClient = new FTPClient();
//
//		try {
//			ftpClient.connect(SERVER);
//			showServerReply(ftpClient);
//
//			int replyCode = ftpClient.getReplyCode();
//			if (!FTPReply.isPositiveCompletion(replyCode)) {
//				System.out.println("Connect failed");
//				return files;
//			}
//
//			boolean success = ftpClient.login(USER_NAME, PASSWORD);
//			showServerReply(ftpClient);
//
//			if (!success) {
//				System.out.println("Could not login to the server");
//				return files;
//			}
//
//			files = listDirectory(ftpClient, url, "", 0);
//		
//			ftpClient.disconnect();
//
//		} catch (IOException ex) {
//			System.out.println("Oops! Something wrong happened");
//			ex.printStackTrace();
//		} finally {
//			// logs out and disconnects from server
//			try {
//				if (ftpClient.isConnected()) {
//					ftpClient.logout();
//					ftpClient.disconnect();
//				}
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}
//		}
//		return files;
//	}
	
//	private static List<String> listDirectory(FTPClient ftpClient, String parentDir,
//			String currentDir, int level) throws IOException {
//
//		List<String> fileNames = new ArrayList<>();
//		String dirToList = parentDir;
//		if (!currentDir.equals("")) {
//			dirToList += "/" + currentDir;
//		}
//		FTPFile[] subFiles = ftpClient.listFiles(dirToList);
//		if (subFiles != null && subFiles.length > 0) {
//			for (FTPFile aFile : subFiles) {
//				String currentFileName = aFile.getName();
//				if (currentFileName.equals(".")
//						|| currentFileName.equals("..")) {
//					// skip parent directory and directory itself
//					continue;
//				}
//				for (int i = 0; i < level; i++) {
//					System.out.print("\t");
//				}
//				if (aFile.isDirectory()) {
//					System.out.println("[" + currentFileName + "]");
//					fileNames.addAll(listDirectory(ftpClient, dirToList, currentFileName, level + 1));
//				} else {
//					fileNames.add(dirToList + "/" + currentFileName);
//					System.out.println(currentFileName);
//				}
//			}
//		}
//		return fileNames;
//	}
//
//	private static void showServerReply(FTPClient ftpClient) {
//		String[] replies = ftpClient.getReplyStrings();
//		if (replies != null && replies.length > 0) {
//			for (String aReply : replies) {
//				System.out.println("SERVER: " + aReply);
//			}
//		}
//	}
	
	private static JavaSparkContext getJavaSparkContext() {
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
