package org.biocaddie.datamention.train;




import java.io.IOException;
import java.io.PrintWriter;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.rcsb.spark.util.SparkUtils;

import scala.Tuple2;

public class PdbDataMentionTrainer {
	private static final String exclusionFilter = "pdb_id != '3DNA' AND pdb_id != '1AND' AND pdb_id NOT LIKE '%H2O'";

	public static void main(String[] args) throws IOException {
		
		String workingDirectory = args[0];
		
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		String positivesIFileName = workingDirectory + "/PositivesI.parquet";
		DataFrame positivesI = sqlContext.read().parquet(positivesIFileName); 
//		long positiveCountI = positivesI.count();
		
		String positivesIIFileName = workingDirectory + "/PositivesII.parquet";
		DataFrame positivesII = sqlContext.read().parquet(positivesIIFileName); 
//		long positiveCountII = positivesII.count();
		
		String negativesIFileName = workingDirectory + "/NegativesI.parquet";
		DataFrame negativesI = sqlContext.read().parquet(negativesIFileName);
//		long negativeCountI = negativesI.count();
		
		String negativesIIFileName = workingDirectory + "/NegativesII.parquet";
		DataFrame negativesII = sqlContext.read().parquet(negativesIIFileName);
//		long negativeCountII = negativesII.count();
		
		String unassignedFileName = workingDirectory + "/Unassigned.parquet";
		DataFrame unassigned = sqlContext.read().parquet(unassignedFileName).filter(exclusionFilter).cache();
//		long unassignedCount = unassigned.count();
		
		long start = System.nanoTime();
		
		String metricsFileName = workingDirectory + "/Metrics.txt";
		PrintWriter writer = new PrintWriter(metricsFileName);
		
		DataFrame positives = positivesI.unionAll(positivesII);
		DataFrame negatives = negativesI.unionAll(negativesII);

		writer.println("*** Positives, Negatives ***");
		
		String modelFileName = workingDirectory + "/Model.ser";
		writer.println(train(sqlContext, positives, negatives, unassigned, modelFileName));
		writer.close();

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}

	private static String train(SQLContext sqlContext, DataFrame positives, DataFrame negatives, DataFrame unassigned, String modelFileName) {
	// combine data sets
	DataFrame all = positives.unionAll(negatives).filter(exclusionFilter);

	// split into training and test sets
	DataFrame[] split = all.randomSplit(new double[]{.8, .2}, 1); 
	DataFrame training = split[0].cache();	
	DataFrame test = split[1].cache();

	// fit logistic regression model
	PipelineModel model = fitLogisticRegressionModel(training);
	
	try {
	     ModelSerializer.serialize(model, modelFileName);
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}


	// predict on training data to evaluate goodness of fit
	DataFrame trainingResults = model.transform(training).cache();

	// predict on test set to evaluate goodness of fit
	DataFrame testResults = model.transform(test).cache();	
	testResults.printSchema();
	
    StringBuilder sb = new StringBuilder();
	sb.append(getMetrics(trainingResults, "Training\n"));
	sb.append(model.explainParams() + "\n");
	
	sb.append(getMetrics(testResults, "Testing\n"));

 //   sb.append("Positive predictions: " + positivePredictions + " out of " + unassigned.count() + " unassigned data mentions\n");
	return sb.toString();
	}
	
	private static PipelineModel fitLogisticRegressionModel(DataFrame training) {
		// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
		Tokenizer tokenizer = new Tokenizer()
		.setInputCol("blinded_sentence")
		.setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
		.setInputCol(tokenizer.getOutputCol())
		.setOutputCol("features");
		LogisticRegression lr = new LogisticRegression()
		.setTol(0.1)
		.setMaxIter(50);
//		.setMaxIter(25);

		Pipeline pipeline = new Pipeline()
		.setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

		// fit the pipeline to training documents.
		PipelineModel model = pipeline.fit(training);
		return model;
	}

	private static String getMetrics(DataFrame predictions, String text) {
		JavaRDD<Tuple2<Object, Object>> scoresAndLabels = predictions.toJavaRDD().map(r -> new Tuple2<Object, Object>(r.getAs("prediction"), r.getAs("label"))).cache();

		BinaryClassificationMetrics metrics1 = new BinaryClassificationMetrics(JavaRDD.toRDD(scoresAndLabels));
        double roc = metrics1.areaUnderROC();

        // Evaluate true/false positives(1)/negatives(0)
        //                                                 prediction             label    
        int tp = scoresAndLabels.map(t -> ((double)t._1 == 1.0 && (double)t._2 == 1.0) ? 1 : 0).reduce((a,b) -> a + b);
        int tn = scoresAndLabels.map(t -> ((double)t._1 == 0.0 && (double)t._2 == 0.0) ? 1 : 0).reduce((a,b) -> a + b);
        int fp = scoresAndLabels.map(t -> ((double)t._1 == 1.0 && (double)t._2 == 0.0) ? 1 : 0).reduce((a,b) -> a + b);
        int fn = scoresAndLabels.map(t -> ((double)t._1 == 0.0 && (double)t._2 == 1.0) ? 1 : 0).reduce((a,b) -> a + b);

        double f1 = 2.0 * tp / (2*tp + fp + fn);
        double fpr = 1.0 * fp /(fp + tn);
        double fnr = 1.0 * fn /(fn + tp);
        double sen = 1.0 * tp /(tp + fn);
        double spc = 1.0 * tn /(tn + fp);
        scoresAndLabels.unpersist();
        StringBuilder sb = new StringBuilder();
        sb.append("Binary Classification Metrics: " + text + "\n");
        sb.append("Roc: " + roc + " F1: " + f1 + " SPC: " + spc + " SEN: " + sen + " FPR: " + fpr + " FNT: " + fnr + " TP: " + tp + " TN: " + tn + " FP: " + fp + " FN: " + fn + "\n");
        return sb.toString();
	}
	
	
}
