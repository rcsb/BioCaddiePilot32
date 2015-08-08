package org.biocaddie.datamention.mine;

import java.io.Serializable;

public class DataMentionRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	private String pdbId;
	private String fileName;
	private String sentence;
	private String blindedSentence;
	private String matchType;
	private Boolean match;

	public DataMentionRecord(String pdbId, String fileName, String sentence, String blindedSentence, String matchType, Boolean match) {
		this.pdbId = pdbId;
		this.fileName = fileName;
		this.sentence = sentence;
		this.blindedSentence = blindedSentence;
		this.matchType = matchType;
		this.match = match;
	}

	public String getPdbId() {
		return pdbId;
	}
	public void setPdbId(String pdbId) {
		this.pdbId = pdbId;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getSentence() {
		return sentence;
	}
	public void setSentence(String sentence) {
		this.sentence = sentence;
	}

	public String getBlindedSentence() {
		return blindedSentence;
	}

	public void setBlindedSentence(String blindedSentence) {
		this.blindedSentence = blindedSentence;
	}

	public String getMatchType() {
		return matchType;
	}

	public void setMatchType(String matchType) {
		this.matchType = matchType;
	}

	public Boolean getMatch() {
		return match;
	}

	public void setMatch(Boolean match) {
		this.match = match;
	}

}

