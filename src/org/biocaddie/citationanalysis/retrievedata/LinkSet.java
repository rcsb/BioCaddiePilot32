package org.biocaddie.citationanalysis.retrievedata;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * This class is used to store the citations of a PubMed id (LinkSet). 
 * It is used within the CitationLinkResultXmlParser.java.
 */
public class LinkSet {
	String id;
    public List<String> citedinLinkIds;
    public List<String> refsLinkIds;
    public StringBuffer citedinLinks;
    public StringBuffer refLinks;    
	Vector<String> inLinks = new Vector<String>();
	Vector<String> outLinks = new Vector<String>();
	
	LinkSet(){
    /*	citedinLinkIds = new ArrayList<String>();
    	refsLinkIds = new ArrayList<String>();*/
    	citedinLinks = new StringBuffer();
    	refLinks = new StringBuffer();
    	
	}
    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getCitedinLinkIds() {
		return citedinLinkIds;
	}
	public void setCitedinLinkIds(List<String> citedinLinkIds) {
		this.citedinLinkIds = citedinLinkIds;
	}
    public List<String> getRefsLinkIds() {
		return refsLinkIds;
	}
	public void setRefsLinkIds(List<String> refsLinkIds) {
		this.refsLinkIds = refsLinkIds;
	}	
	public void clear() {
		this.id = "";
		this.citedinLinks.setLength(0);
		this.refLinks.setLength(0);
	}
}