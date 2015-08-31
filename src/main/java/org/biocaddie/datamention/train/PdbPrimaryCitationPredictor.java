package org.biocaddie.datamention.train;

import java.io.IOException;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.DataFrameToDelimitedFileWriter;
import org.rcsb.spark.util.SparkUtils;


public class PdbPrimaryCitationPredictor {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		String workingDirectory = args[0];
		
		String positivesIIFileName = workingDirectory + "/" + "PositivesII.parquet";
		DataFrame positivesII = sqlContext.read().parquet(positivesIIFileName).cache();
		positivesII.show(5);
		
		String unassignedFileName = workingDirectory + "/" + "Unassigned.parquet";
		DataFrame unassigned = sqlContext.read().parquet(unassignedFileName).cache();
		unassigned.show(5);
		DataFrame union = positivesII.unionAll(unassigned);
		
		String modelFileName = workingDirectory + "/" + "PdbPrimaryCitationModel.ser";
		DataFrame predicted = predict(sqlContext, union, modelFileName);
		
		String predictionParquetFileName = workingDirectory + "/" + "PdbPrimaryCitationPredicted.parquet";
		predicted.write().format("parquet").mode(SaveMode.Overwrite).save(predictionParquetFileName);
		
		String predictionTsvFileName = workingDirectory + "/" + "PdbPrimaryCitationPredicted.tsv";
	    DataFrameToDelimitedFileWriter.writeTsv(predictionTsvFileName, predicted);

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}

	private static DataFrame predict(SQLContext sqlContext, DataFrame unassigned, String modelFileName) throws ClassNotFoundException, IOException {
		PipelineModel model = (PipelineModel)ObjectSerializer.deserialize(modelFileName);

		// predict on data mentions
		DataFrame predictionResults = model.transform(unassigned).cache();	
		sqlContext.registerDataFrameAsTable(predictionResults, "prediction");

		DataFrame predicted = sqlContext.sql("SELECT h.pdb_id, h.match_type, h.deposition_year, h.pmc_id, h.pm_id, h.primary_citation, h.publication_year, h.sentence FROM prediction h WHERE h.prediction = 1.0").cache();

		return predicted;
	}
}
