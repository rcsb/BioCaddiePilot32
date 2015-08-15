package org.biocaddie.datamention.train;




import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		DataFrame positivesI = sqlContext.read().parquet(args[0]);
		sqlContext.registerDataFrameAsTable(positivesI, "positivesI");
		DataFrame positives = sqlContext.sql("SELECT pdbId, matchType, depositionYear, pmcId, pmId, publicationYear, CAST(primaryCitation AS double) AS label, sentence, blindedSentence  FROM positivesI");
		positives.show(10);
		//		long positiveCountI = positivesI.count();

		DataFrame positivesII = sqlContext.read().parquet(args[1]); 
		sqlContext.registerDataFrameAsTable(positivesII, "positivesII");
		DataFrame negatives = sqlContext.sql("SELECT pdbId, matchType, depositionYear, pmcId, pmId, publicationYear, CAST(primaryCitation AS double) AS label, sentence, blindedSentence FROM positivesII WHERE sentence NOT LIKE '%deposited%' AND sentence NOT LIKE '%submitted%'");
		negatives.show(10);
		//		long positiveCountII = positivesII.count();

		long start = System.nanoTime();

		PrintWriter writer = new PrintWriter(args[3]);

		writer.println("*** Positives, Negatives ***");
		writer.println(train(sqlContext, positives, negatives, args[2]));
		writer.close();

		long end = System.nanoTime();
		System.out.println("Time: " + (end-start)/1E9 + " sec.");

		sc.stop();
	}

	private static String train(SQLContext sqlContext, DataFrame positives, DataFrame negatives, String modelFileName) {
		// combine data sets
		DataFrame all = positives.unionAll(negatives);

		// split into training and test sets
		DataFrame[] split = all.randomSplit(new double[]{.75, .25}, 2); // changed seed from 1 to 2!!
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

		// predict on unassigned data mentions

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
		.setInputCol("blindedSentence")
		.setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
		.setNumFeatures(100000)
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
		scoresAndLabels.unpersist();
		StringBuilder sb = new StringBuilder();
		sb.append("Binary Classification Metrics: " + text + "\n");
		sb.append("Roc: " + roc + " F1: " + f1 + " FPR: " + fpr + " TP: " + tp + " TN: " + tn + " FP: " + fp + " FN: " + fn + "\n");
		return sb.toString();
	}

	private static SparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(PdbPrimaryCitationTrainer.class.getSimpleName())
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