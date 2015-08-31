package org.biocaddie.datamention.analysis;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.rcsb.spark.util.SparkUtils;

public class CompareResults {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		System.out.println(Arrays.toString(args));
		
		// read PDB data mentions
		DataFrame mentionsAll = sqlContext.read().parquet(args[0]).cache(); 
		DataFrame mentionsPdb = mentionsAll.select("pmcId").distinct().cache();
		System.out.println("inPdbAll: " + mentionsPdb.count());
		
		//read PMC data: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
		DataFrame pmc = sqlContext.read().parquet(args[1]);
		DataFrame oaPmc = pmc.select("pmcId").cache();
		sqlContext.registerDataFrameAsTable(oaPmc, "oaPmc");
		System.out.println("oaPmc: " + oaPmc.count());

		// PDB Data mentions from PMC Europe
		DataFrame mentionsPmcE = sqlContext.read().parquet(args[2]).distinct().cache(); 
		sqlContext.registerDataFrameAsTable(mentionsPmcE, "mentionsPmcE");

		System.out.println("mentionsPmcE: " + mentionsPmcE.count());
		DataFrame mentionsPmcOa = sqlContext.sql("SELECT m.pmcId FROM mentionsPmcE m INNER JOIN oaPmc o ON m.pmcId=o.pmcId").cache();
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
}
