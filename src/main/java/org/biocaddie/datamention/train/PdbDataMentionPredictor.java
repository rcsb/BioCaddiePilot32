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
 * This class predicts PDB Data Mentions as either positive or negative data mention.
 * 
 * @author Peter Rose
 *
 */
public class PdbDataMentionPredictor {
	private static final String exclusionFilter = "pdb_id != '3DNA' AND pdb_id != '1AND' AND pdb_id NOT LIKE '%H2O'";

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		String workingDirectory = args[0];
		
		String unassignedFileName = workingDirectory + "/Unassigned.parquet";
		DataFrame unassigned = sqlContext.read().parquet(unassignedFileName).filter(exclusionFilter);
		
		String modelFileName = workingDirectory = "/Model.ser";
		DataFrame predicted = predict(sqlContext, unassigned, modelFileName);
		
		String predictedFileName = workingDirectory + "/Predicted.parquet";
		predicted.write().mode(SaveMode.Overwrite).parquet(predictedFileName);
		
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

		DataFrame predicted = sqlContext.sql("SELECT h.pdb_id, h.match_type, h.deposition_year, h.pmc_id, h.pm_id, h.publication_year, h.primary_citation, h.sentence, h.blinded_sentence, h.label FROM prediction h WHERE h.prediction = 1.0").cache();

		return predicted;
	}
}
