package org.biocaddie.datamention.mine;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.rcsb.spark.util.SparkUtils;

/**
 * This class generates positive and negative training/test sets for classifying PDB Data Mentions
 * using Machine Learning methods.
 * 
 * An Unassigned file is also generated that contains PDB Data Mentions that could not be
 * classified as either positive or negative. Machine learning models will be used to classify
 * these data mentions.
 * 
 * @author Peter Rose
 *
 */
public class PdbDataMentionTrainingSetGenerator
{
	private static final String PDB_DATA_MENTIONS = "PdbDataMentions.parquet";
	private static final String PMC_FILE_METATDATA = "PmcFileMetadata.parquet";
	private static final String PMC_ARTICLE_METADATA = "PmcArticleMetadata.parquet";
	private static final String PDB_CURRENT_METADATA = "PdbCurrentMetaData.parquet";
	private static final String PDB_OBSOLETE_METADATA = "PdbObsoleteMetaData.parquet";
		
	public static void main(String args[]) throws Exception
	{
		long start = System.nanoTime();
		
		String workingDirectory = args[0];

		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		SQLContext sqlContext = SparkUtils.getSqlContext(sc);
		int threads = sc.defaultParallelism();

		// Read data sources
		System.out.println(Arrays.toString(args));

		// dataMentions: pdbId, fileName, sentence, matchType;
		String dataMentionFileName = workingDirectory + "/" + PDB_DATA_MENTIONS;
		DataFrame dataMentions = sqlContext.read().parquet(dataMentionFileName).repartition(threads).cache();
		System.out.println("PDB Data Mentions : " + dataMentions.count());
		sqlContext.registerDataFrameAsTable(dataMentions, "dataMentions");
		
		// pmcFileMetadata: pmcId, fileName, lastUpdated
		String pmcFileMetadataFileName = workingDirectory + "/" + PMC_FILE_METATDATA;
		DataFrame pmc1 = sqlContext.read().parquet(pmcFileMetadataFileName).cache();
		System.out.println("PMC File Metadata: " + pmc1.count());
		
		// pmcArticleMetadata: pmcId, pmId, publicationYear
		String pmcArticleMetadataFileName = workingDirectory + "/" + PMC_ARTICLE_METADATA;
		DataFrame pmc2 = sqlContext.read().parquet(pmcArticleMetadataFileName);
		System.out.println("PMC Article Metadata: " + pmc2.count());
		
		DataFrame pmc = pmc1.join(pmc2, "pmc_id").repartition(threads).cache();
		sqlContext.registerDataFrameAsTable(pmc, "pmc");
		
		pmc1.unpersist();
		pmc2.unpersist();
		
		System.out.println("PMC Joint Metadata: " + pmc.count());
		pmc.printSchema();
		pmc.show();

		DataFrame superset = sqlContext.sql(
				"SELECT m.*, p.publication_year, p.pmc_id, p.pm_id FROM dataMentions m LEFT OUTER JOIN pmc p ON m.file_name=p.file_name WHERE p.pmc_id IS NOT null").cache();
		superset.repartition(threads);
		sqlContext.registerDataFrameAsTable(superset, "superset");
		System.out.println("Joint PDB Data Mentions with PMC Metadata: " + superset.count());
		System.out.println(superset.schema());
		dataMentions.unpersist();
		pmc.unpersist();

		// citations: pdbId, pmcId, pmId, depositionYear, depositionDate, entryType;
		String pdbCurrentMetadataFileName = workingDirectory + "/" + PDB_CURRENT_METADATA;
		DataFrame currentEntries = sqlContext.read().parquet(pdbCurrentMetadataFileName).cache();
		System.out.println("PDB Current Metadata: " + currentEntries.count());
		
		String pdbObsoleteMetadataFileName = workingDirectory + "/" + PDB_OBSOLETE_METADATA;
		DataFrame obsoleteEntries = sqlContext.read().parquet(pdbObsoleteMetadataFileName).cache();
		DataFrame pdbMetadata = currentEntries.unionAll(obsoleteEntries).repartition(threads).cache();
		sqlContext.registerDataFrameAsTable(pdbMetadata, "pdbMetadata");
		currentEntries.unpersist();
		obsoleteEntries.unpersist();

		int nullCount = 0;
		
		// look for both pmcId and pmID matches
		DataFrame merged = sqlContext.sql(
				"SELECT s.pdb_id, s.sentence, s.blinded_sentence, s.match_type, s.pmc_id, s.pm_id, s.publication_year, c.deposition_year, c.entry_type, CASE WHEN c.pmc_id IS NOT NULL AND (c.pmc_id=s.pmc_id OR c.pm_id=s.pm_id) THEN true ELSE false END as primary_citation FROM superset s LEFT OUTER JOIN pdbMetadata c ON s.pdb_id=c.pdb_id").cache();
		merged.repartition(threads);
		sqlContext.registerDataFrameAsTable(merged, "merged");
		superset.unpersist();

		System.out.println("Null count: " + nullCount);

		DataFrame positivesI = sqlContext.sql(
				"SELECT m.pdb_id, m.match_type, m.deposition_year, m.pmc_id, m.pm_id, m.publication_year, m.primary_citation, m.sentence, m.blinded_sentence, 1.0 as label FROM merged m WHERE m.primary_citation = 1 AND m.publication_year >= m.deposition_year").cache();
		String positivesIFileName = workingDirectory + "/PositivesI.parquet";
		positivesI.coalesce(threads).write().mode(SaveMode.Overwrite).parquet(positivesIFileName);
		System.out.println("positiveI: " + positivesI.count());
		
		DataFrame positivesII = sqlContext.sql(
				"SELECT m.pdb_id, m.match_type, m.deposition_year, m.pmc_id, m.pm_id, m.publication_year, m.primary_citation, m.sentence, m.blinded_sentence, 1.0 as label FROM merged m WHERE m.match_type!='PDB_NONE' AND m.primary_citation = 0 AND m.publication_year>=m.deposition_year").cache();

		System.out.println("positiveII: " + positivesII.count());
		String positivesIIFileName = workingDirectory + "/PositivesII.parquet";
		positivesII.coalesce(threads).write().mode(SaveMode.Overwrite).parquet(positivesIIFileName);

		DataFrame negativesI = sqlContext.sql(
				"SELECT m.pdb_id, m.match_type, m.deposition_year, m.pmc_id, m.pm_id, m.publication_year, m.primary_citation, m.sentence, m.blinded_sentence, 0.0 as label FROM merged m WHERE m.entry_type IS NOT null AND m.publication_year<m.deposition_year").cache();
		String negativesIFileName = workingDirectory + "/NegativesI.parquet";
		negativesI.coalesce(threads).write().mode(SaveMode.Overwrite).parquet(negativesIFileName);		
		System.out.println("negativesI: " + negativesI.count());

		DataFrame negativesII = sqlContext.sql(
				"SELECT m.pdb_id, m.match_type, m.deposition_year, m.pmc_id, m.pm_id, m.publication_year, m.primary_citation, m.sentence, m.blinded_sentence, 0.0 as label FROM merged m WHERE m.entry_type IS null").cache();
		String negativesIIFileName = workingDirectory + "/NegativesII.parquet";
		negativesII.coalesce(threads).write().mode(SaveMode.Overwrite).parquet(negativesIIFileName);			
		System.out.println("negativeII: " + negativesII.count());

		DataFrame unassignedMentions = sqlContext.sql(
				"SELECT m.pdb_id, m.match_type, m.deposition_year, m.pmc_id,m.pm_id, m.publication_year, m.primary_citation, m.sentence, m.blinded_sentence, 0.0 as label FROM merged m WHERE m.entry_type IS NOT NULL AND m.match_type='PDB_NONE' AND m.publication_year>=m.deposition_year AND m.primary_citation=false").cache();

		System.out.println("Unassigned Data Mentions: " + unassignedMentions.schema());
		String unassignedMentionsFileName = workingDirectory + "/Unassigned.parquet";
		unassignedMentions.coalesce(threads).write().mode(SaveMode.Overwrite).parquet(unassignedMentionsFileName);	

		System.out.println("time: " + (System.nanoTime()-start)/1E9 + " s");

		sc.stop();
	}
}
