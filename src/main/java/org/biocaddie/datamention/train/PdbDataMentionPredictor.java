package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * 
 * @author Peter Rose
 *
 */
public class PdbDataMentionPredictor {
	private static final String exclusionFilter = "pdbId != '3DNA' AND pdbId != '1AND' AND pdbId NOT LIKE '%H2O'";

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		DataFrame unassigned = sqlContext.read().parquet(args[0]).filter(exclusionFilter);
//		long unassignedCount = unassigned.count();
		
		System.out.println("Using model: " + args[1]);
		DataFrame predicted = predict(sqlContext, unassigned, args[1]);
		predicted.write().mode(SaveMode.Overwrite).parquet(args[3]);
		
	    try {
	    	DataFrameToDelimitedFileWriter.write(args[2], "\t", predicted);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}

	/**
	 * Predict ....
	 * @param sqlContext 
	 * @param unassigned dataframe with unassigned date
	 * @param modelFileName
	 * @return
	 */
	private static DataFrame predict(SQLContext sqlContext, DataFrame unassigned, String modelFileName) {
		PipelineModel model = null;
		try {
			model = (PipelineModel)ModelSerializer.deserialize(modelFileName);
		} catch (IOException | ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// predict on data mentions
		DataFrame predictionResults = model.transform(unassigned).cache();	
		sqlContext.registerDataFrameAsTable(predictionResults, "prediction");
		System.out.println(predictionResults.schema());

		DataFrame predicted = sqlContext.sql("SELECT h.pdbId, h.matchType, h.depositionYear, h.pmcId, h.pmId, h.publicationYear, h.primaryCitation, h.sentence, h.blindedSentence, h.label FROM prediction h WHERE h.prediction = 1.0").cache();

		return predicted;
	}
}
