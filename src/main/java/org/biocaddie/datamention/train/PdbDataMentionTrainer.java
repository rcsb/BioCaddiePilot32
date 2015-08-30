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
	private static final String exclusionFilter = "pdbId != '3DNA' AND pdbId != '1AND' AND pdbId NOT LIKE '%H2O'";
	private static final int NUM_PARTITIONS = 4;

	public static void main(String[] args) throws IOException {
		// Set up contexts.
		SparkContext sc = SparkUtils.getSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		// read positive data set, cases where the PDB ID occurs in the sentence of the primary citation
		DataFrame positivesI = sqlContext.read().parquet(args[0]); 
//		long positiveCountI = positivesI.count();
		
		DataFrame positivesII = sqlContext.read().parquet(args[1]); 
//		long positiveCountII = positivesII.count();
		
		DataFrame negativesI = sqlContext.read().parquet(args[2]);
//		long negativeCountI = negativesI.count();
		
		DataFrame negativesII = sqlContext.read().parquet(args[3]);
//		long negativeCountII = negativesII.count();
		
		DataFrame unassigned = sqlContext.read().parquet(args[4]).filter(exclusionFilter).cache();
//		long unassignedCount = unassigned.count();
		
		long start = System.nanoTime();
		
		PrintWriter writer = new PrintWriter(args[5]);
		
//		if (verbose) {
//			writer.println("*** PositivesI, NegativesI ***\n");
//			writer.println(train(sqlContext, positivesI, negativesI, unassigned, args[6]+"P1N1.tsv"));
//			writer.flush();
//
//			writer.println("*** PositivesII, NegativesI ***");
//			writer.println(train(sqlContext, positivesII, negativesI, unassigned, args[6]+"P2N1.tsv"));
//			writer.flush();
//
//			writer.println("*** PositivesI, NegativesII ***");
//			writer.println(train(sqlContext, positivesI, negativesII, unassigned, args[6]+"P1N2.tsv"));
//			writer.flush();
//
//			writer.println("*** PositivesII, NegativesII ***");
//			writer.println(train(sqlContext, positivesII, negativesII, unassigned, args[6]+"P2N2.tsv"));
//			writer.flush();
//		}
		
		DataFrame positives = positivesI.unionAll(positivesII);
		DataFrame negatives = negativesI.unionAll(negativesII);
//		DataFrame negatives = negativesI.unionAll(negativesII).sample(false, 0.1, 1);
		writer.println("*** Positives, Negatives ***");
		writer.println(train(sqlContext, positives, negatives, unassigned, args[6], args[7]));
		writer.close();

	    long end = System.nanoTime();
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
		sc.stop();
	}

	private static String train(SQLContext sqlContext, DataFrame positives, DataFrame negatives, DataFrame unassigned, String fileName, String modelFileName) {
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
		.setInputCol("blindedSentence")
		.setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
	//	.setNumFeatures(100000)
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
//		for (Row r: predictions.collect()) {
//			  System.out.println("prob=" + r.getAs("probability"));
//		}
		JavaRDD<Tuple2<Object, Object>> scoresAndLabels = predictions.toJavaRDD().map(r -> new Tuple2<Object, Object>(r.getAs("prediction"), r.getAs("label"))).cache();
//		JavaRDD<Tuple2<Object, Object>> scoresAndLabels = predictions.toJavaRDD().map(r -> new Tuple2<Object, Object>(((DenseVector)r.getAs("probability")).toArray()[0], r.getAs("label"))).cache();

		BinaryClassificationMetrics metrics1 = new BinaryClassificationMetrics(JavaRDD.toRDD(scoresAndLabels));
        double roc = metrics1.areaUnderROC();
//        for (Tuple2<Object, Object> t: metrics1.recallByThreshold().toJavaRDD().collect()) {
//        	System.out.println(t);
//        }
        
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
