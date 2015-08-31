package org.biocaddie.datamention.train;

import java.io.FileNotFoundException;
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

public class PdbPrimaryCitationTrainer {

	public static void main(String[] args) throws FileNotFoundException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		String workingDirectory = args[0];
		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		String positivesIFileName = workingDirectory + "/" + "PositivesI.parquet";
		DataFrame positivesI = sqlContext.read().parquet(positivesIFileName);
		
		sqlContext.registerDataFrameAsTable(positivesI, "positivesI");
		DataFrame positives = sqlContext.sql("SELECT pdb_id, match_type, deposition_year, pmc_id, pm_id, publication_year, CAST(primary_citation AS double) AS label, sentence, blinded_sentence  FROM positivesI");
		positives.show(10);

		String positivesIIFileName = workingDirectory + "/" + "PositivesII.parquet";
		DataFrame positivesII = sqlContext.read().parquet(positivesIIFileName); 
		System.out.println("Sampling 16% of positivesII");
		positivesII = positivesII.sample(false,  0.16, 1);
		sqlContext.registerDataFrameAsTable(positivesII, "positivesII");
		DataFrame negatives = sqlContext.sql("SELECT pdb_id, match_type, deposition_year, pmc_id, pm_id, publication_year, CAST(primary_citation AS double) AS label, sentence, blinded_sentence FROM positivesII WHERE sentence NOT LIKE '%deposited%' AND sentence NOT LIKE '%submitted%'");
		negatives.show(10);

		long start = System.nanoTime();

		String metricFileName = workingDirectory + "/" + "PdbPrimaryCitationMetrics.txt";
		PrintWriter writer = new PrintWriter(metricFileName);

		writer.println("PDB Primary Citation Classification: Logistic Regression Results");
		
		String modelFileName = workingDirectory + "/" + "PdbPrimaryCitationModel.ser";
		writer.println(train(sqlContext, positives, negatives, modelFileName));
		writer.close();

		long end = System.nanoTime();
		System.out.println("Time: " + (end-start)/1E9 + " sec.");

		sc.stop();
	}

	private static String train(SQLContext sqlContext, DataFrame positives, DataFrame negatives, String modelFileName) {
		// combine data sets
		DataFrame all = positives.unionAll(negatives);

		// split into training and test sets
		DataFrame[] split = all.randomSplit(new double[]{.80, .20}, 1);
		DataFrame training = split[0].cache();	
		DataFrame test = split[1].cache();

		// fit logistic regression model
		PipelineModel model = fitLogisticRegressionModel(training);

		try {
			ObjectSerializer.serialize(model, modelFileName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// predict on training data to evaluate goodness of fit
		DataFrame trainingResults = model.transform(training).cache();

		// predict on test set to evaluate goodness of fit
		DataFrame testResults = model.transform(test).cache();	

		// predict on unassigned data mentions

		StringBuilder sb = new StringBuilder();
		sb.append(getMetrics(trainingResults, "Training\n"));

		sb.append(getMetrics(testResults, "Testing\n"));

		return sb.toString();
	}

	private static PipelineModel fitLogisticRegressionModel(DataFrame training) {
		// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.

		Tokenizer tokenizer = new Tokenizer()
		.setInputCol("blinded_sentence")
		.setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
//		.setNumFeatures(100000)
		.setInputCol(tokenizer.getOutputCol())
		.setOutputCol("features");
		LogisticRegression lr = new LogisticRegression()
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
