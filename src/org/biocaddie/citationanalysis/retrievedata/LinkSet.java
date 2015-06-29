package org.biocaddie.citationanalysis.retrievedata;

/**
 * This class is used to store the citations of a PubMed id (LinkSet). 
 * It is used within the CitationLinkResultXmlParser.java.
 */
public class LinkSet {
	String id;
    public StringBuffer citedinLinks;
    public StringBuffer refLinks;    
	
	LinkSet(){
    	citedinLinks = new StringBuffer();
    	refLinks = new StringBuffer();   	
	}
    
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void clear() {
		this.id = "";
		this.citedinLinks.setLength(0);
		this.refLinks.setLength(0);
	}
}