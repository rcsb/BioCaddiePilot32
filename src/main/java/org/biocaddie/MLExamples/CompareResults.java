package org.biocaddie.MLExamples;



import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import edu.emory.mathcs.backport.java.util.Arrays;

public class CompareResults {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = getSparkContext();
		SQLContext sqlContext = getSqlContext(sc);

		System.out.println(Arrays.toString(args));
		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		DataFrame mentionsAll = sqlContext.read().parquet(args[0]).cache(); 
		DataFrame mentionsPdb = mentionsAll.select("pmcId").distinct().cache();
		System.out.println("inPdbAll: " + mentionsPdb.count());
		
		// pmc: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
		DataFrame pmc = sqlContext.read().parquet(args[1]);
		DataFrame oaPmc = pmc.select("pmcId").cache();
		sqlContext.registerDataFrameAsTable(oaPmc, "oaPmc");
		System.out.println("oaPmc: " + oaPmc.schema());
		System.out.println("oaPmc: " + oaPmc.count());
//		for (Row r: oaPmc.collect()) {
//			System.out.println(r);
//		}
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("header", "true");
		options.put("delimiter", ",");
		options.put("mode", "DROPMALFORMED");
	    options.put("inferSchema", "true"); // this option does not work
		DataFrame mentionsPmcE = sqlContext.read().parquet(args[2]).distinct().cache(); 
		sqlContext.registerDataFrameAsTable(mentionsPmcE, "mentionsPmcE");
		System.out.println("mentionPmcE: " + mentionsPmcE.schema());
//		for (Row r: mentionsPmcE.collect()) {
//	    	System.out.println(r);
//	    }
		System.out.println("mentionsPmcE: " + mentionsPmcE.count());
		DataFrame mentionsPmcOa = sqlContext.sql("SELECT m.pmcId FROM mentionsPmcE m INNER JOIN oaPmc o ON m.pmcId=o.pmcId").cache();
//        DataFrame mentionsPmcOa = mentionsPmcE.join(oaPmc).cache();
		System.out.println("mentionsPmcOa: " + mentionsPmcOa.count());
		
		DataFrame inPdbOnly = mentionsPdb.except(mentionsPmcOa).cache();
		System.out.println("inPdbOnly: " + inPdbOnly.count());
		for (Row r: inPdbOnly.collect()) {
		    System.out.println(r);
	    }
		DataFrame inPmcEOnly = mentionsPmcOa.except(mentionsPdb).cache();
		System.out.println("inPmcEOnly: " + inPmcEOnly.count());
		for (Row r: inPmcEOnly.collect()) {
			System.out.println(r);
		}
	    
		sc.stop();
	}
	
	private static SparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(CompareResults.class.getSimpleName())
		.set("spark.driver.maxResultSize", "4g")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "1g");

		SparkContext sc = new SparkContext(conf);

		return sc;
	}
	
	private static SQLContext getSqlContext(SparkContext sc) {
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		return sqlContext;
	}
}
