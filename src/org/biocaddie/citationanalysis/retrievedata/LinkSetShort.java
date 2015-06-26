package org.biocaddie.citationanalysis.retrievedata;

import java.util.HashMap;
import java.util.Map;

public class LinkSetShort {
	public Integer id;
	public Map<Integer, Integer> inLinks = new HashMap<Integer, Integer>();
	public Map<Integer, Integer> outLinks = new HashMap<Integer, Integer>();
	public LinkSetShort(){
	}
	
}
