package org.biocaddie.PDBTools;



import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rahul on 3/16/15.
 */
public class PDBFinder implements Serializable {
    private static final long serialVersionUID = 1L;
    /*
     * General pattern to match a pdb id. Note, we only allow all upper case or all lower case patterns
     * Examples: 1XYZ, 1xyz, 1x2z, 123z
     */
    private static final Pattern PDB_PATTERN = Pattern.compile("[1-9]([A-Z0-9]{3}|[a-z0-9]{3})");
 
    private static final Pattern ID_PATTERN = Pattern.compile("\\b(PDB ID:|pdb id:|PDBID|pdbid|PDB_ID|pdb_id)");
    private static final Pattern PDB_ID_PATTERN = Pattern.compile(ID_PATTERN.pattern() + "." + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern PDB_DOI_PATTERN = Pattern.compile("10.2210/pdb" + PDB_PATTERN.pattern() + "/pdb");
    private static final Pattern PDB_ENTRY_PATTERN = Pattern.compile("\\b" + PDB_PATTERN.pattern() + "entry" + "\\b");
    private static final Pattern PDB_FILE_PATTERN = Pattern.compile("\\b" + PDB_PATTERN.pattern() + ".pdb" + "\\b");
    private static final Pattern RCSB_PDB_URL_PATTERN1 = Pattern.compile("explore.do.structureId=" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern RCSB_PDB_URL_PATTERN2 = Pattern.compile("explore.cgi.pdbId="+ PDB_PATTERN.pattern() + "\\b");
    private static final Pattern RCSB_PDB_URL_PATTERN3 = Pattern.compile("structidSearch.do.structureId="+ PDB_PATTERN.pattern() + "\\b");
//    public static final Pattern EXT_LINK_PATTERN = Pattern.compile("<ext-link ext-link-type=\"pdb\" xlink:href=\"" + PDB_PATTERN.pattern());
    private static final Pattern PDB_NONE_PATTERN = Pattern.compile("\\b" + PDB_PATTERN.pattern() + "\\b");
    /*
     * PDB ID in href='...."
     */
    private static final Pattern PDB_EXT_LINK_PATTERN = Pattern.compile("ext-link-type=\"pdb\" xlink:href=\""
            + PDB_PATTERN.pattern()
            + "\">");

    /*
     * PDB skip one word pattern: PDB code 1XYZ, pdb code: 1xyz, PDB assession 1xyz, ...
     */
    private static final Pattern PDB_SKIP_0_PATTERN = Pattern.compile("\\b(pdb|PDB)\\W*" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern PDB_SKIP_1_PATTERN = Pattern.compile("\\b(pdb|PDB)\\W*\\w+\\W*" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern PDB_SKIP_2_PATTERN = Pattern.compile("\\b(pdb|PDB)\\W*\\w+\\W*\\w+\\W*" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern ACCESSION_SKIP_0_PATTERN = Pattern.compile("\\b(accession|Accession)\\W*" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern ACCESSION_SKIP_1_PATTERN = Pattern.compile("\\b(accession|Accession)\\W*\\w+\\W*" + PDB_PATTERN.pattern() + "\\b");
    private static final Pattern ACCESSION_SKIP_2_PATTERN = Pattern.compile("\\b(accession|Accession)\\W*\\w+\\W*\\w+\\W*" + PDB_PATTERN.pattern() + "\\b");
    /*
     * Exclusion patterns
     */
    private static final Pattern DIGITS = Pattern.compile("\\d{4}");
	private static final char dot = '·';
	private static final char period = '.';
	
    private static Map<String, Pattern> patterns = new LinkedHashMap<String, Pattern>();

    static {
        patterns.put("PDB_ID", PDB_ID_PATTERN);
//        patterns.put("PDB_DOI", PDB_DOI_PATTERN); // this one doesn't match
        patterns.put("PDB_ENTRY", PDB_ENTRY_PATTERN);
        patterns.put("PDB_FILE", PDB_FILE_PATTERN);
        patterns.put("RCSB_PDB_URL1", RCSB_PDB_URL_PATTERN1); // previously found ~600, this one doesn't match! PMC2483516: 1WI1 ((http://www.rcsb.org/pdb/explore.do?structureId=1WI1)
        patterns.put("RCSB_PDB_URL2", RCSB_PDB_URL_PATTERN2);
        patterns.put("RCSB_PDB_URL3", RCSB_PDB_URL_PATTERN3); 
        patterns.put("PDB_EXT_LINK", PDB_EXT_LINK_PATTERN); // !17K, should this be more?? previously, found 32K??
        // out of order tags? ext-link xlink:href=http://www.rcsb.org/pdb/explore/explore.do?structureId=1MU2 ext-link-type="pdb"&gt;
        // legacy url http://www.rcsb.org/pdb/cgi/explore.cgi?pdbId=1DLH
        // http://www.rcsb.org/pdb/search/structidSearch.do?structureId=3SDM
        // also www.pdb.org!
        patterns.put("PDB_SKIP_0", PDB_SKIP_0_PATTERN);
        patterns.put("PDB_SKIP_1", PDB_SKIP_1_PATTERN);
        patterns.put("PDB_SKIP_2", PDB_SKIP_2_PATTERN);
        patterns.put("ACCESSION_SKIP_0", ACCESSION_SKIP_0_PATTERN);
        patterns.put("ACCESSION_SKIP_1", ACCESSION_SKIP_1_PATTERN);
        patterns.put("ACCESSION_SKIP_2", ACCESSION_SKIP_2_PATTERN);
        patterns.put("PDB_NONE", PDB_NONE_PATTERN);
    }
    
    public static void main(String[] args) {
//    	String url = "http://www.rcsb.org/pdb/explore.do?structureId=2wuy";
//    	System.out.println(getPdbMatchType(url));
//    	String url1 = "As an unexpected result of our study, we observed an inhibitory KIR (2DL2) more frequently in non-recurrent patients.";
//    	System.out.println(getPdbMatchType(url1));
//    	System.out.println(PDB_DOI_PATTERN.toString());
   // 	Pattern PDB_DOI_PATTERN = Pattern.compile("10.2210/pdb" + PDB_PATTERN.pattern() + "/pdb");
    	String doi = "10.2210/pdb4cf7/pdb";
//    	System.out.println(PDB_DOI_PATTERN.matcher(doi));
//    	Matcher matcher = PDB_DOI_PATTERN.matcher(doi);
//		if (matcher.find()) {
//			System.out.println(matcher.group());
//		}
    	System.out.println(containsPdbId(doi));
    	System.out.println(getPdbMatchType(doi));
    	System.out.println(getPdbIds(doi));
    	System.out.println(containsPdbIdFast("deposited 1XYZ sdfjksdfl"));
    	System.out.println();
    }
    
	public static boolean containsPdbId(String document) {
//		if (! containsPdbIdFast(document)) {
//			return false;
//		}
		Matcher matcher = PDB_NONE_PATTERN.matcher(document);
		while (matcher.find()) {
			if (!DIGITS.matcher(matcher.group()).find()) {
				return true;
			} 
		}
		if (document.contains("10.2210/pdb")) {
    		Matcher doiMatcher = PDB_DOI_PATTERN.matcher(document);
    		if (doiMatcher.find()) {
                return true;
    		}
    	}
		return false;
	}
	
	public static Set<String> getPdbIds(String sentence) {
		Matcher matcher = PDB_NONE_PATTERN.matcher(sentence);
		Set<String> pdbIds = new TreeSet<>();

		while (matcher.find()) {
			if (! DIGITS.matcher(matcher.group()).find()) {
				int start = matcher.start();
				if (start > 0) {
					char prefix = sentence.charAt(start-1);
					// exclude matches that have a period or dot as prefix since they are found
					// chemical formulas: .2H2O
					// floating point numbers: 0.123d 
					// measurements: 0.01mm, .3ppm, 0.4min, .3sec				
					if (prefix == period || prefix == dot) {
//						System.out.println(sentence.substring(start-1, start+4));
						continue;
					}
				}

				String pdbId = matcher.group().toUpperCase();
				if (! pdbIds.contains(pdbId)) {
					pdbIds.add(pdbId);
				}
			}
		}
		if (sentence.contains("10.2210/pdb")) {
    		Matcher doiMatcher = PDB_DOI_PATTERN.matcher(sentence);
    		if (doiMatcher.find()) {
                String doi = doiMatcher.group().substring(11,15).toUpperCase();
//                System.out.println(doi);
                pdbIds.add(doi);
    		}
    	}
		return pdbIds;
	}

    public static String getPdbMatchType(String line) {
    	if (line.contains("10.2210/pdb")) {
    		Matcher matcher = PDB_DOI_PATTERN.matcher(line);
    		if (matcher.find()) {
//    			System.out.println(matcher.group().substring(11,15));
    			return "PDB_DOI";
    		}
    	}
        for (Map.Entry<String, Pattern> pattern : patterns.entrySet()) {
        	Matcher p = pattern.getValue().matcher(line);
 //       	System.out.println(pattern.getValue());
            if (p.find()) {
 //           	System.out.println("getPdbatchType: found: " + p.group() + pattern.getValue() + ": " + pattern.getKey());
            	if (DIGITS.matcher(p.group()).find()) {
   //         		System.out.println("matched an int: " + p.group());
            		continue;
            	}
            	// TODO check for 4-digit PDB id here
                return pattern.getKey();
            }
        }
 //       return null;
        return "PDB_NONE";
    }

    public static boolean isPositivePattern(String type) {
    	return !(type.equals("PDB_NONE"));
    }

	private static boolean containsPdbIdFast(String document) {
		boolean lowercase = true;
		
		for (int i = 0; i < document.length()-4; i++) {
			char c = document.charAt(i);
			int digits = 1;
			//1. digit
			if (c > '0' && c <= '9') {
				// 2. digit or letter
				i++;
				c = document.charAt(i);
				if (c >= '0' && c <= '9') {
					digits++;
				} else if (c >= 'a' && c <= 'z') {
					lowercase = true;
				} else if (c >= 'A' && c <= 'Z') {
					lowercase = false;
				} else {
					continue;
				}
				i++;
				c = document.charAt(i);
				// 3. digit or letter
				if (c >= '0' && c <= '9') {
					digits++;
				} else if (c >= 'a' && c <= 'z') {
					if (!lowercase) {
						continue;
					}
				} else if (c >= 'A' && c <= 'Z') {
					if (lowercase) {
					    continue;
					}
				}
				i++;
				c = document.charAt(i);
				// 4. digit or letter
				if (c >= '0' && c <= '9') {
					digits++;
				} else if (c >= 'a' && c <= 'z') {
					if (!lowercase) {
						continue;
					}
				} else if (c >= 'A' && c <= 'Z') {
					if (lowercase) {
					    continue;
					}
				}
				if (digits == 4) {
					continue;
				}
				return true;
			}
		}
		return false;
	}

}
