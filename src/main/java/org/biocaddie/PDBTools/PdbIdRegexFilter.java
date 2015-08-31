package org.biocaddie.PDBTools;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


/*Spark Java programming APIs*/
import java.util.regex.Matcher;
import java.util.Set;
/**
 * PdbIdRegexFilter applies a regular expression for a PDB ID and checks if
 * the matched PDB ID is a currently used PDB ID. This is a filter class that 
 * is to be passed to the JavaRDD.filter() function.
 * 
 * @author Peter Rose, Rahul Palamuttam
 */
public class PdbIdRegexFilter implements Function<Tuple2<String, String>, Boolean> {
	private static final long serialVersionUID = 1L;
	/*
	 * PDB ID and partial PDB DOI regular expression
	 */
	private static final Pattern PDB_GENERAL_PATTERN = Pattern.compile("(\\b|pdb|PDB)[1-9][A-Za-z0-9]{3}\\b");
	/*
	 * List of currently used PDB IDs that are shared among compute nodes
	 */
    private Broadcast<Set<String>> pdbIds;
    
    /**
     * Constructor takes Broadcast object that contains the list of all current PDB IDs. 
     * The Broadcast object is shared across all compute nodes.
     * @param pdbIds list of current PDB IDs
     */
    public PdbIdRegexFilter(Broadcast<Set<String>> pdbIds){
	   this.pdbIds = pdbIds;
    }
    
    /**
     * Returns true if text document matches PDB ID regular expression and is a current PDB ID
     * @param document tuple that represents a document name / document content pair
     */
    public Boolean call(Tuple2<String, String> document){
		Matcher matcher = PDB_GENERAL_PATTERN.matcher(document._2);
		
		while (matcher.find()){
    		String s = matcher.group();
    		if (s.length() == 7) {
    			s = s.substring(3,7);
    		}
    		
			if (pdbIds.value().contains(s.toUpperCase())) {
				return true;
			}
		}
		return false;
	} 
}
