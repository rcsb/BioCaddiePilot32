package org.biocaddie.datamention.mine;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.biocaddie.datamention.download.PmcFileEntry;
import org.rcsb.spark.util.SparkUtils;

public class PdbDataMentionTrainingSetGeneratorNew
{
	private static int NUM_PARTITIONS = 4;
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();

		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
//		sc.getConf().registerKryoClasses(new Class[]{PmcFileEntry.class});
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);

		// Read data sources
		System.out.println(Arrays.toString(args));

		// dataMentions: pdbId, fileName, sentence, matchType;
		DataFrame dataMentions = sqlContext.read().parquet(args[0]).cache();
		System.out.println("Mention records: " + dataMentions.count());
		sqlContext.registerDataFrameAsTable(dataMentions, "Mentions");
		
		// pmcFileMetadata: pmcId, fileName, lastUpdated
		System.out.println("reading:  " + args[1]);
		DataFrame pmc1 = sqlContext.read().parquet(args[1]);
	//	System.out.println("pmcFileMetadata records: " + pmc1.count());
		
		// pmcArticleMetadata: pmcId, pmId, publicationYear
		System.out.println("reading:  " + args[2]);
		DataFrame pmc2 = sqlContext.read().parquet(args[2]);
		System.out.println("pmcArticleMetadata records: " + pmc2.count());
		
		DataFrame pmc = pmc1.join(pmc2, "pmcId").cache();
		sqlContext.registerDataFrameAsTable(pmc, "Pmc");
		System.out.println("pmc: " + pmc.count());
		pmc.printSchema();
		pmc.show();

		// pmc: fileName, citation, pmcId, pmId, publicationYear, publicationDate, updateDate;
//		DataFrame pmc = sqlContext.read().parquet(args[1]).cache();
//		System.out.println("Pmc records: " + pmc.count());
//		sqlContext.registerDataFrameAsTable(pmc, "Pmc");
//		for (Row r: pmc.collect()) {
//			if (r.getAs("pmcId") == null) {
//			System.out.println(r);
//			}
//		}
		

		DataFrame superSet1 = sqlContext.sql(
				"SELECT m.*, p.publicationYear, p.pmcId, p.pmId FROM Mentions m LEFT OUTER JOIN Pmc p ON m.fileName=p.fileName WHERE p.pmcId IS NOT null").cache();
		sqlContext.registerDataFrameAsTable(superSet1, "SuperSet1");
		System.out.println("superSet1 size: " + superSet1.count());
		System.out.println(superSet1.schema());
		sqlContext.uncacheTable("Mentions");
		sqlContext.uncacheTable("Pmc");

		// citations: pdbId, pmcId, pmId, depositionYear, depositionDate, entryType;
		DataFrame currentEntries = sqlContext.read().parquet(args[3]).cache();
		System.out.println("Current PDB records: " + currentEntries.count());
		
		DataFrame obsoleteEntries = sqlContext.read().parquet(args[4]).cache();
		DataFrame pdbCitations = currentEntries.unionAll(obsoleteEntries).cache();
		sqlContext.registerDataFrameAsTable(pdbCitations, "Citations");

//		DataFrame merged = sqlContext.sql(
//				"SELECT s.pdbId, s.sentence, s.blindedSentence, s.matchType, s.pmcId, s.pmId, s.publicationYear, c.depositionYear, c.entryType, CASE WHEN c.pmcId IS NOT NULL AND c.pmcId=s.pmcId THEN true ELSE false END as primaryCitation FROM SuperSet1 s LEFT OUTER JOIN Citations c ON s.pdbId=c.pdbId").cache();
//		sqlContext.registerDataFrameAsTable(merged, "Merged");
//		sqlContext.uncacheTable("Citations");
//		sqlContext.uncacheTable("SuperSet1");
//		System.out.println("merged by pmcId: " + merged.count());
//		System.out.println("merged prim. cit.: " + merged.filter("primaryCitation = true").count());
//		System.out.println(merged.schema());
		int nullCount = 0;
		
		// look for both pmcId and pmID matches
		DataFrame merged = sqlContext.sql(
				"SELECT s.pdbId, s.sentence, s.blindedSentence, s.matchType, s.pmcId, s.pmId, s.publicationYear, c.depositionYear, c.entryType, CASE WHEN c.pmcId IS NOT NULL AND (c.pmcId=s.pmcId OR c.pmId=s.pmId) THEN true ELSE false END as primaryCitation FROM SuperSet1 s LEFT OUTER JOIN Citations c ON s.pdbId=c.pdbId").cache();
		sqlContext.registerDataFrameAsTable(merged, "Merged");
		sqlContext.uncacheTable("Citations");
		sqlContext.uncacheTable("SuperSet1");

		System.out.println("Null count: " + nullCount);

		DataFrame positivesI = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 1.0 as label FROM Merged m WHERE m.primaryCitation = 1 AND m.publicationYear >= m.depositionYear").cache();
		positivesI.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[5]);
		System.out.println("positiveI: " + positivesI.count());
		
		DataFrame positivesII = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 1.0 as label FROM Merged m WHERE m.matchType!='PDB_NONE' AND m.primaryCitation = 0 AND m.publicationYear>=m.depositionYear").cache();

		System.out.println("positiveII: " + positivesII.count());
		positivesII.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[6]);

		DataFrame negativesI = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS NOT null AND m.publicationYear<m.depositionYear").cache();
		negativesI.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[7]);		
		System.out.println("negativesI: " + negativesI.count());

		DataFrame negativesII = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId, m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS null").cache();
		negativesII.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[8]);			
		System.out.println("negativeII: " + negativesII.count());

		DataFrame validMentions = sqlContext.sql(
				"SELECT m.pdbId, m.matchType, m.depositionYear, m.pmcId,m.pmId, m.publicationYear, m.primaryCitation, m.sentence, m.blindedSentence, 0.0 as label FROM Merged m WHERE m.entryType IS NOT NULL AND m.matchType='PDB_NONE' AND m.publicationYear>=m.depositionYear AND m.primaryCitation=false").cache();


		System.out.println("mentions: " + validMentions.schema());
		
		validMentions.coalesce(NUM_PARTITIONS).write().mode(SaveMode.Overwrite).parquet(args[9]);	

		System.out.println("time: " + (System.nanoTime()-start)/1E9 + " s");

		sc.stop();
	}
}
