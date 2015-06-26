package org.biocaddie.citationanalysis.retrievedata;

/**
 * This class is used to store the summary attributes(DocSum) of a PubMed Journal. We only keep the attributes we needed.
 * It is used within the CitationSummaryResultXmlParser.java.
 */
public class DocSum {
	
	String id;
	String title;
	String lastAuthor;
	String pubDate;		
	String nlmUniqueID; // unique journal id
	String fullJournalName;

	DocSum(){ // constructor method
    }
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getLastAuthor() {
		return lastAuthor;
	}

	public void setLastAuthor(String lastAuthor) {
		this.lastAuthor = lastAuthor;
	}

	public String getPubDate() {
		return pubDate;
	}

	public void setPubDate(String pubDate) {
		this.pubDate = pubDate;
	}

	public String getNlmUniqueID() {
		return nlmUniqueID;
	}

	public void setNlmUniqueID(String nlmUniqueID) {
		this.nlmUniqueID = nlmUniqueID;
	}

	public String getFullJournalName() {
		return fullJournalName;
	}

	public void setFullJournalName(String fullJournalName) {
		this.fullJournalName = fullJournalName;
	}

	public void clear() {
		this.id = "";
		this.title = "";
		this.lastAuthor = "";
		this.pubDate = "";
		this.nlmUniqueID = "";
		this.fullJournalName = "";
	}
	
}
