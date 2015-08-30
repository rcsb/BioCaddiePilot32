package org.biocaddie.datamention.analysis;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class PmcArticleHasher implements Function<byte[], Vector> {
	private static final long serialVersionUID = 1L;;
	private static List<String> stopWords;
	static {
		stopWords = loadStopWords();
	}

	@Override
	public Vector call(byte[] data) throws Exception {
		String document = new String(data, Charset.forName("UTF-8"));
		document = removeXmlTags(document);
		document = trimWhiteSpace(document);
		document = removeStopWords(document);
		
		List<String> words = new ArrayList<>();
		String[] items = document.split(" ");
		for (String w: items) {
			if (w.length() >= 4) {
				words.add(w);
//				System.out.println("w: " + w + "|");
			}
		}
        HashingTF tf = new HashingTF(1000);
        Vector htf = tf.transform(words);
        double[] array = htf.toArray();
        for (int i = 0; i < array.length; i++) {
        	if (array[i] > 0) {
        		array[i] = 1;
        	}
        }
        return Vectors.dense(array);
		
//		return htf;
	}

	private static String trimWhiteSpace(String sentence) {
//		sentence = sentence.replaceAll("\\[^a-zA-Z\\d\\s:]", " ");
//		sentence = sentence.replaceAll("[^a-zA-Z :]", " ");
		sentence = sentence.replaceAll("[^a-zA-Z]", " ");
//		sentence = sentence.replaceAll("\n", " ");
//		sentence = sentence.replaceAll("\t", " ");
//        sentence = sentence.replaceAll("\\.", " ");
//        sentence = sentence.replaceAll(",", " ");
        
//		int length;
//		do {
//			length = sentence.length();
//			sentence = sentence.replaceAll("  ", " ");
//
//		} while (sentence.length() < length);
		return sentence;
	}
	
	public static String removeXmlTags(String sentence) {	
		return sentence.replaceAll("<.*?>", " ");
	}

	private static List<String> loadStopWords() {
		List<String> stopWords = new ArrayList<String>();
		BufferedReader reader = null;
		try {
		reader = new BufferedReader(new InputStreamReader(PmcArticleHasher.class.getResourceAsStream("/stopword.csv")));
		String line = null;
			while ((line = reader.readLine()) != null) {
				stopWords.add(line.trim());
//				System.out.println(line);
			}
			reader.close();
		} catch (IOException e) {
			System.err.println("Error loading stopwords " + e.getMessage());
		}

		return stopWords;
	}

	private static String removeStopWords(String sentence) {
		String lcSentence = sentence.toLowerCase();
		for (String s: stopWords) {
			lcSentence = lcSentence.replaceAll("\\b" + s + "\\b", "");
		}
//		System.out.println(lcSentence);
		return lcSentence;
	}
}
