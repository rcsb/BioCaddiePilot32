package org.biocaddie.datamention.download;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class uses RCSB PDB Tabular Report web services to retrieve data and saves them
 * as Spark DataFrame in the .parquet file. It renames columns to have consistent
 * names throughout the entire toolset.
 * 
 * @author Peter Rose
 *
 */
public class TabularReportToParquet {
	public static final String SERVICELOCATION="http://www.rcsb.org/pdb/rest/customReport";
	private static final String CURRENT_URL = "?pdbids=*&service=wsfile&format=csv&primaryOnly=1&customReportColumns=";
//pmc,pubmedId,depositionDate
	public static void main(String[] args) throws IOException {
		String outputFileName = args[0];
		
		TabularReportToParquet downloader = new TabularReportToParquet();

		downloader.createTempFile(outputFileName, Arrays.asList("pmc","pubmedId","depositionDate"));
	}
	
//	public DataFrame createTabularReport(List<String> columnNames) {
//           String url = getUrl(columnNames);
//	}
	
	public void createTempFile(String outputFileName, List<String> columnNames) throws IOException {
		String query = getUrl(columnNames);
		System.out.println("query: " + query);
		
		InputStream input = postQuery(query);
		Path tempFile = saveTempFile(input);
		downloadCsv(tempFile.toString(), outputFileName);
	}
	
	/** post pdbids and fields in a query string to the RESTful RCSB web service
	*
	* @param url
	* @return report dataset.
	*/
   	public InputStream postQuery(String url) throws IOException
	{
		URL u = new URL(SERVICELOCATION);

		String encodedUrl = URLEncoder.encode(url,"UTF-8");

		InputStream input =  doPOST(u,encodedUrl);
		
		return input;
	}

    private Path saveTempFile(InputStream input) throws IOException {
		
		Path tempFile = null;

	    tempFile = Files.createTempFile(null, ".csv");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
	    System.out.println("The temporary file" + tempFile + " has been created.");
		
		input.close();
		
		return tempFile;
	}

   	/** do a POST to a URL and return the response stream for further processing elsewhere.
	*
	*
	* @param url
	* @return
	* @throws IOException
	*/
	public static InputStream doPOST(URL url, String data)
		throws IOException
   	{

		System.out.println("url: " + url + " data: " + data);
		// Send data
		URLConnection conn = url.openConnection();
		conn.setConnectTimeout(60000);
		
		conn.setDoOutput(true);
		
		OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());

		wr.write(data);
		wr.flush();

		// Get the response
		return conn.getInputStream();
	}
	
	public String getUrl(List<String> columnNames) {
		StringBuilder sb = new StringBuilder();
		sb.append(CURRENT_URL);
		
		for (int i = 0; i < columnNames.size(); i++) {
			sb.append(columnNames.get(i));
			if (i != columnNames.size()-1) {
			   sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	private static void downloadCsv(String inputFileName, String outputFileName) throws IOException {
		
	    System.out.println(inputFileName + " -> " + outputFileName);
		
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
	    SQLContext sql = SparkUtils.getSqlContext(sc);
	    
	    // for options see https://github.com/databricks/spark-csv
	    DataFrame df = sql.read()
	    		.format("com.databricks.spark.csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(inputFileName);
	        
        df.printSchema();
        df.show(5);
       
	    df.write().format("parquet").mode(SaveMode.Overwrite).save(outputFileName);
	}

}
