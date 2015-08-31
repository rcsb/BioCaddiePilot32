package org.biocaddie.datamention.mine;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biocaddie.PDBTools.PDBFinder;

import scala.Tuple2;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * This class maps a tuple (fileName, document) to a list of PDB data mention records.
 * The document is represented by a byte array that contains text encoded in UTF-8 format
 * and can either be in XML or plain text format.
 * 
 * @author Peter Rose
 *
 */
public class PdbDataMentionMapper implements FlatMapFunction<Tuple2<String, byte[]>, DataMentionRecord> {
	private static final long serialVersionUID = 1L;
	private static StanfordCoreNLP textPipeline;
//	private static List<String> stopWords;
	static {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit");
		textPipeline = new StanfordCoreNLP(props);
//		stopWords = loadStopWords(); are not used currently
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

		List<CoreMap> sentences = splitIntoSentences(document, fileName);

		for (CoreMap cmSentence : sentences) {
			String sentence = cmSentence.toString();

			if (PDBFinder.containsPdbId(sentence)) {
				Set<String> pdbIds = PDBFinder.getPdbIds(sentence);

				String matchType = PDBFinder.getPdbMatchType(sentence);
				Boolean match = PDBFinder.isPositivePattern(matchType);
				sentence = removeXmlTags(sentence);
				String tSentence = trimExtraCharacters(sentence);
				String bSentence = getBlindedSentence(pdbIds, tSentence);
				// bSentence = removeStopWords(bSentence); // removing stop words makes no difference, for now leaving it out

				for (String pdbId: pdbIds) {
					DataMentionRecord record = new DataMentionRecord(pdbId, fileName, tSentence, bSentence, matchType, match);
					list.add(record);
				}
			} 
		}
		return list;
	}

/**
 * Splits a document into sentences using the Stanford NLP library.
 * 
 * @param document
 * @param fileName
 * @return List of sentences encoded in CoreMap objects
 */
	private static List<CoreMap> splitIntoSentences(String document, String fileName) {
		List<CoreMap> sentences = Collections.emptyList();
		Annotation annotation = new Annotation(document);
		try {
			textPipeline.annotate(annotation);
		} catch (Exception e) {
			System.out.println(fileName + ": " + e.getMessage());
			return sentences;
		}

		sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
		
		return sentences;
	}

	/**
	 * Trims newlines, tabs, and extra spaces from a sentence.
	 * @param sentence
	 * @return
	 */
	private static String trimExtraCharacters(String sentence) {
		sentence = sentence.replaceAll("\n", " ");
		sentence = sentence.replaceAll("\t", " ");
		int length;
		do {
			length = sentence.length();
			sentence = sentence.replaceAll("  ", " ");

		} while (sentence.length() < length);
		
		return sentence.trim();
	}
	
	public static String removeXmlTags(String sentence) {	
		return sentence.replaceAll("<.*?>", " ");
	}

	private static String getBlindedSentence(Set<String> pdbIds, String sentence) {
		String blindedSentence = new String(sentence);
		blindedSentence = blindedSentence.toLowerCase();
		for (String pdbId: pdbIds) {
			String lcPdbId = pdbId.toLowerCase();
			blindedSentence = blindedSentence.replaceAll(lcPdbId, "XXXX");
		}
		return blindedSentence;
	}

// for potential future use
//
//	private static List<String> loadStopWords() {
//		List<String> stopWords = new ArrayList<String>();
//		BufferedReader reader = null;
//		try {
//		reader = new BufferedReader(new InputStreamReader(PdbDataMentionMapper.class.getResourceAsStream("/stopword.csv")));
//		String line = null;
//			while ((line = reader.readLine()) != null) {
//				stopWords.add(line.trim());
//				System.out.println(line);
//			}
//			reader.close();
//		} catch (IOException e) {
//			System.err.println("Error loading stopwords " + e.getMessage());
//		}
//
//		return stopWords;
//	}
//
//	private static String removeStopWords(String sentence) {
//		String lcSentence = sentence.toLowerCase();
//		for (String s: stopWords) {
//			lcSentence = lcSentence.replaceAll("\\b" + s + "\\b", "");
//		}
//		return lcSentence;
//	}
}
