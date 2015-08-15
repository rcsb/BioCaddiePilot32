package org.biocaddie.datamention.mine;



import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biocaddie.PDBTools.PDBFinder;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import scala.Tuple2;

public class PdbDataMentionMapper implements FlatMapFunction<Tuple2<String, byte[]>, DataMentionRecord> {
	private static final long serialVersionUID = 1L;
	private static StanfordCoreNLP textPipeline;
	static {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit"); // does order mapper, moved cleanxml first
//		props.setProperty("clean.xmltags", ".*"); // this doesn't work
//		props.setProperty("clean.allowflawedxml", "true"); // this doesn't work
		textPipeline = new StanfordCoreNLP(props);
	}

	@Override
	public Iterable<DataMentionRecord> call(Tuple2<String, byte[]> tuple) throws Exception {
		String document = new String(tuple._2,Charset.forName("UTF-8"));
		List<DataMentionRecord> records = Collections.emptyList();
		String fileName = tuple._1;
		records = getPdbSentences(document, fileName);

		return records;
	}

	private static List<DataMentionRecord> getPdbSentences(String document, String fileName) {	
		List<DataMentionRecord> list = new ArrayList<>();

//		document = removeXmlTags(document); 
		Annotation annotation = new Annotation(document);
		try {
			textPipeline.annotate(annotation);
		} catch (Exception e) {
			System.out.println(fileName + ": " + e.getMessage());
			return list;
		}

		List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

		if (sentences != null) {
			for (CoreMap cmSentence : sentences) {
				String sentence = cmSentence.toString();
// (PDBFinder.containsPdbId(sentence) || sentence.contains("10.2210/pdb")) {
					if (PDBFinder.containsPdbId(sentence)) {
					Set<String> pdbIds = PDBFinder.getPdbIds(sentence);
					
					String matchType = PDBFinder.getPdbMatchType(sentence);
					Boolean match = PDBFinder.isPositivePattern(matchType);
					sentence = removeXmlTags(sentence);
					String tSentence = trimNewLines(sentence);
					String bSentence = getBlindedSentence(pdbIds, tSentence);

					for (String pdbId: pdbIds) {
						DataMentionRecord record = new DataMentionRecord(pdbId, fileName, tSentence, bSentence, matchType, match);
						list.add(record);
					}
				} 
			}
		}
		return list;
	}

	private static String trimNewLines(String sentence) {
		sentence = sentence.replaceAll("\n", " ");
		sentence = sentence.replaceAll("\t", " ");
		int length;
		do {
			length = sentence.length();
			sentence = sentence.replaceAll("  ", " ");

		} while (sentence.length() < length);
		return sentence;
	}
	
	public static void printXmlTags(String sentence) {
		if (sentence.contains("<ext-link ext-link-type=\"pdb\" xlink:href=\"") ) {
        String matchType = PDBFinder.getPdbMatchType(sentence);
//		if (sentence.contains("<ext-link ext-link-type=\"pdb\" xlink:href=\"....\">") ) {
//		if (sentence.contains("<ext-link ext-link-type=\"pdb\" xlink:href=\"") ) {
//	if (matchType.equals("EXT_LINK")) {
			System.out.println(matchType + ": " + sentence);
		}
	}
	public static String removeXmlTags(String sentence) {	
		return sentence.replaceAll("<.*?>", " ");
	}

	private static String getBlindedSentence(Set<String> pdbIds, String sentence) {
		String blindedSentence = new String(sentence);
		for (String pdbId: pdbIds) {
			blindedSentence = blindedSentence.replaceAll(pdbId, "XXXX");
			String lcPdbId = pdbId.toLowerCase();
			blindedSentence = blindedSentence.replaceAll(lcPdbId, "XXXX");
		}
		return blindedSentence;
	}
}
