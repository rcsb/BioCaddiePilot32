package org.biocaddie.DataConverters;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class PdbDataMentionTrainingSetGenerator
{
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();

		JavaSparkContext sc = getSparkContext();
		SQLContext sqlContext = getSqlContext(sc);

		// Read data sources

		// dataMentions: pdbId, fileName, sentence, matchType;
		DataFrame dataMentions = sqlContext.read().parquet(args[0]).cache();
		System.out.println("Mention records: " + dataMentions.count());
		sqlContext.registerDataFrameAsTable(dataMentions, "Mentions");

		// pmc: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
		DataFrame pmc = sqlContext.read().parquet(args[1]).cache();
		System.out.println("Pmc records: " + pmc.count());
		sqlContext.registerDataFrameAsTable(pmc, "Pmc");
		for (Row r: pmc.collect()) {
			if (r.getAs("pmcId") == null) {
			System.out.println(r);
			}
		}
		

		DataFrame superSet1 = sqlContext.sql(
				"SELECT m.*, p.publicationYear, p.pmcId, p.pmId FROM Mentions m LEFT OUTER JOIN Pmc p ON m.fileName=p.fileName WHERE p.pmcId IS NOT null").cache();
		sqlContext.registerDataFrameAsTable(superSet1, "SuperSet1");
		System.out.println("superSet1 size: " + superSet1.count());
		System.out.println(superSet1.schema());
		sqlContext.uncacheTable("Mentions");
		sqlContext.uncacheTable("Pmc");

		// citations: pdbId, pmcId, pmId, depositionYear, depositionDate, entryType;
//		DataFrame pdbCitations = sqlContext.read().parquet(args[2]).cache();
		DataFrame currentEntries = sqlContext.read().parquet(args[2]).cache();
		System.out.println("Current PDB records: " + currentEntries.count());
		
		DataFrame obsoleteEntries = sqlContext.read().parquet(args[3]).cache();
		DataFrame pdbCitations = currentEntries.unionAll(obsoleteEntries).cache();
		sqlContext.registerDataFrameAsTable(pdbCitations, "Citations");

		DataFrame merged = sqlContext.sql(
				"SELECT s.pdbId, s.sentence, s.blindedSentence, s.matchType, s.pmcId, s.pmId, s.publicationYear, c.depositionYear, c.entryType, CASE WHEN c.pmcId IS NOT NULL AND c.pmcId=s.pmcId THEN true ELSE false END as primaryCitation FROM SuperSet1 s LEFT OUTER JOIN Citations c ON s.pdbId=c.pdbId").cache();
		sqlContext.registerDataFrameAsTable(merged, "Merged");
		sqlContext.uncacheTable("Citations");
		sqlContext.uncacheTable("SuperSet1");
		System.out.println("merged size: " + merged.count());
		System.out.println(merged.schema());
		int nullCount = 0;

		System.out.println("Null count: " + nullCount);

		DataFrame positivesI = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 1.0 as label FROM Merged m WHERE m.primaryCitation = 1 AND m.publicationYear >= m.depositionYear").cache();
		positivesI.coalesce(40).write().mode(SaveMode.Overwrite).parquet(args[4]);
		System.out.println("positiveI: " + positivesI.count());

//		DataFrame positivesII = sqlContext.sql(
//				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 1.0 as label FROM Merged m WHERE m.matchType!='PDB_NONE' AND m.publicationYear>=m.depositionYear").cache();
        // avoid duplicates with positivesI:
		
		DataFrame positivesII = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 1.0 as label FROM Merged m WHERE m.matchType!='PDB_NONE' AND m.primaryCitation = 0 AND m.publicationYear>=m.depositionYear").cache();

		System.out.println("positiveII: " + positivesII.count());
		positivesII.coalesce(40).write().mode(SaveMode.Overwrite).parquet(args[5]);

		DataFrame negativesI = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS NOT null AND m.publicationYear<m.depositionYear").cache();
		negativesI.coalesce(40).write().mode(SaveMode.Overwrite).parquet(args[6]);		
		System.out.println("negativesI: " + negativesI.count());

		DataFrame negativesII = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS null").cache();
		negativesII.coalesce(40).write().mode(SaveMode.Overwrite).parquet(args[7]);			
		System.out.println("negativeII: " + negativesII.count());

//		DataFrame validMentions = sqlContext.sql(
//				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId,m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS NOT NULL AND m.publicationYear>=m.depositionYear").cache();
		
		DataFrame validMentions = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId,m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS NOT NULL AND m.matchType='PDB_NONE' AND m.publicationYear>=m.depositionYear").cache();


		System.out.println("mentions: " + validMentions.schema());
		
		validMentions.coalesce(40).write().mode(SaveMode.Overwrite).parquet(args[8]);	

		System.out.println("time: " + (System.nanoTime()-start)/1E9 + " s");

		sc.stop();
	}

	
	private static JavaSparkContext getSparkContext() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		int cores = Runtime.getRuntime().availableProcessors();
		System.out.println("Available cores: " + cores);
		SparkConf conf = new SparkConf()
		.setMaster("local[" + cores + "]")
		.setAppName(PdbDataMentionTrainingSetGenerator.class.getSimpleName())
		.set("spark.driver.maxResultSize", "4g")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryoserializer.buffer.max", "1g");

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
	}
	
	private static SQLContext getSqlContext(JavaSparkContext sc) {
		SQLContext sqlContext = new SQLContext(sc);
		sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");
		sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");
		return sqlContext;
	}

}
