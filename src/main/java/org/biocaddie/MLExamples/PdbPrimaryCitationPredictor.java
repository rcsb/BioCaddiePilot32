package org.biocaddie.MLExamples;




import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;


public class PdbPrimaryCitationPredictor {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		long start = System.nanoTime();
		
		SparkContext sc = getSparkContext();
		SQLContext sqlContext = getSqlContext(sc);
		
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
	
	private static SparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(PdbPrimaryCitationPredictor.class.getSimpleName())
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
