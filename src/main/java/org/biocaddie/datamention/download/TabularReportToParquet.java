package org.biocaddie.datamention.download;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
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
		TabularReportToParquet downloader = new TabularReportToParquet();

		downloader.createTempFile(Arrays.asList("pmc","pubmedId","depositionDate"));
	}
	
//	public DataFrame createTabularReport(List<String> columnNames) {
//           String url = getUrl(columnNames);
//	}
	
	public void createTempFile(List<String> columnNames) throws IOException {
		String query = getUrl(columnNames);
		System.out.println("query: " + query);
		
		postQuery(query);
	}
	
	/** post pdbids and fields in a query string to the RESTful RCSB web service
	*
	* @param url
	* @return report dataset.
	*/
   	public void postQuery(String url)
		throws IOException
	{

		URL u = new URL(SERVICELOCATION);

		String encodedUrl = URLEncoder.encode(url,"UTF-8");

		InputStream in =  doPOST(u,encodedUrl);


		BufferedReader rd = new BufferedReader(new InputStreamReader(in));
		
		
		String line;
		while ((line = rd.readLine()) != null) 
		{
			System.out.println(line);
		}
		rd.close();

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
	private static void downloadCsv(String fileName) throws IOException {
		int index = fileName.lastIndexOf(".csv");
		if (index < 1) {
			throw new IOException("Invalid .csv file name: " + fileName);
		}
		
		String outputFileName = fileName.substring(0,index) + ".parquet";
	    System.out.println(fileName + " -> " + outputFileName);
		
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
	    SQLContext sql = SparkUtils.getSqlContext(sc);
	    
	    // for options see https://github.com/databricks/spark-csv
	    DataFrame df = sql.read()
	    		.format("com.databricks.spark.csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(fileName);
	    
	    df = standardizeColumnNames(df);
	    
	    df = addFileNameColumn(sql, df);
	    
	    df = addLastUpdatedColumn(sql, df);
	   
	    // for now we only need these columns
	    df = df.select("pmcId","pmId", "fileName", "lastUpdated");
	    
        df.printSchema();
        df.show(5);
       
	    df.write().format("parquet").mode(SaveMode.Overwrite).save(outputFileName);
	}

	private static DataFrame standardizeColumnNames(DataFrame df) {
		System.out.println("Original column names: " + Arrays.toString(df.columns()));
	    df = df.withColumnRenamed("File", "file")
	    		.withColumnRenamed("Article Citation", "articleCitation")
	    		.withColumnRenamed("Accession ID", "pmcId")
	    		.withColumnRenamed("Last Updated (YYYY-MM-DD HH:MM:SS)", "updateDate")
	    	    .withColumnRenamed("PMID", "pmId");

		System.out.println("Standardized column names: " + Arrays.toString(df.columns()));
		return df;
	}
	
	/**
	 * Adds a fileName column to the data frame. It trims the path and file extension (.tar.gz) from the 
	 * file column and adds it as the fileName column to the data frame.
	 * 
	 * Example:
	 * file: "08/e0/Breast_Cancer_Res_2001_Nov_2_3(1)_55-60.tar.gz" -> fileName: "Breast_Cancer_Res_2001_Nov_2_3(1)_55-60"
 	 *
	 * @param sql
	 * @param df
	 * @return
	 */
	private static DataFrame addFileNameColumn(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("trim", (String s) -> s.substring(6,s.lastIndexOf(".tar")), DataTypes.StringType);
        df = sql.sql("SELECT d.*, trim(d.file) as fileName FROM df as d");
		return df;
	}
	
	/**
	 * Adds a lastUpdated column to the data frame. It converts the updateDate column to a timestamp 
	 * format and and adds it as lastUpdated column to the data frame. It then delete the updateDate column.
	 * 
	 * Example:
	 * updateDate: "2013-03-19 14:51:52" -> lastUpdated: "2013-03-19"
 	 *
	 * @param sql
	 * @param df
	 * @return
	 */
	private static DataFrame addLastUpdatedColumn(SQLContext sql, DataFrame df) {
		sql.registerDataFrameAsTable(df, "df");
        sql.udf().register("toDate", (String s) -> Timestamp.valueOf(s), DataTypes.TimestampType);
        df = sql.sql("SELECT d.*, toDate(d.updateDate) as lastUpdated FROM df as d");
        df = df.drop("updateDate");
		return df;
	}
	
	
}
