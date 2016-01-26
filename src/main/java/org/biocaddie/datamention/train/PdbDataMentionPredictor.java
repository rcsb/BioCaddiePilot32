package org.biocaddie.datamention.train;

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

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		String workingDirectory = args[0];
		
		String unassignedFileName = workingDirectory + "/Unassigned.parquet";
		DataFrame unassigned = sqlContext.read().parquet(unassignedFileName).filter(exclusionFilter);
		
		String modelFileName = workingDirectory + "/PdbDataMentionModel.ser";
		DataFrame predicted = predict(sqlContext, unassigned, modelFileName);
		
		String predictedFileName = workingDirectory + "/PdbDataMentionPredicted.parquet";
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
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	private static DataFrame predict(SQLContext sqlContext, DataFrame unassigned, String modelFileName) throws ClassNotFoundException, IOException {
		PipelineModel model = (PipelineModel)ObjectSerializer.deserialize(modelFileName);

		// predict on data mentions
		DataFrame predictedResults = model.transform(unassigned).cache();	
		sqlContext.registerDataFrameAsTable(predictedResults, "predictedResults");

		// select positive predictions
		DataFrame predicted = sqlContext.sql("SELECT h.pdb_id, h.match_type, h.deposition_year, h.pmc_id, h.pm_id, h.publication_year, h.primary_citation, h.sentence, h.blinded_sentence, h.label FROM predictedResults h WHERE h.prediction = 1.0").cache();

		return predicted;
	}
}
