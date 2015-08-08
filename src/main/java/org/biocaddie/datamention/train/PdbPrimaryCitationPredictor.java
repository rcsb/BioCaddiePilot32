package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;


public class PdbPrimaryCitationPredictor {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		
		DataFrame positivesII = sqlContext.read().parquet(args[0]).cache();
		positivesII.show(5);
		DataFrame unassigned = sqlContext.read().parquet(args[1]).cache();
		unassigned.show(5);
		DataFrame union = positivesII.unionAll(unassigned);
		
		DataFrame predicted = predict(sqlContext, union, args[2]);
		predicted.write().mode(SaveMode.Overwrite).parquet(args[3]);
		
	    try {
	    	DataFrameToDelimitedFileWriter.writeTsv(args[4], predicted);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}

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

		DataFrame predicted = sqlContext.sql("SELECT h.pdbId, h.matchType, h.depositionYear, h.pmcId, h.pmId, h.primaryCitation, h.publicationYear, h.sentence FROM prediction h WHERE h.prediction = 1.0").cache();
//		long positivePredictions = predicted.count();
		//	predicted.coalesce(1).write().mode(SaveMode.Overwrite).parquet(fileName);
		return predicted;
	}
}
