package org.biocaddie.citationanalysis.utility;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DrugTargetCorrelation {
    final static Charset ENCODING = StandardCharsets.UTF_8;

	public static void main(String[] args) throws Exception {
		
		//Step 1: Read PDB csv file, and generate a map of (GenericName <-> PDB Id) where similarity >= 90
		readPDBDrugTargetCSVFile();
		
	}

	public static Map<String, Map<String, Integer>> readPDBDrugTargetCSVFile() throws Exception {
		System.out.println("test");

		Map<String, Map<String, Integer>> pdb_drug_map = new HashMap<String, Map<String, Integer>>(); //(PDBID -> (GenericName -> %similarity))	
    	BufferedReader reader3 = Files.newBufferedReader(Paths.get("/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/pdb_drug_target/pdb_drugTable2.csv"), ENCODING);
    	String line = null;
		while ((line = reader3.readLine()) != null) {			
			//skip the header line and any empty lines
			if (line.trim().startsWith("Generic") || line.trim().equals(""))
				continue;
	    	
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String generic_name = tokens[0].substring(1,tokens[0].length()-1);
			String pdb_id_1 = tokens[6].substring(1, tokens[6].length()-1);       String pdb_id_1_similarity = tokens[7]; 
			String pdb_id_2 = tokens[8].substring(1, tokens[8].length()-1);       String pdb_id_2_similarity = tokens[9];
			String pdb_id_3 = tokens[10].substring(1, tokens[10].length()-1);     String pdb_id_3_similarity = tokens[11];
			
			add_to_pdb_drug_map(generic_name, pdb_id_1, pdb_id_1_similarity, pdb_drug_map);
			add_to_pdb_drug_map(generic_name, pdb_id_2, pdb_id_2_similarity, pdb_drug_map);
			add_to_pdb_drug_map(generic_name, pdb_id_3, pdb_id_3_similarity, pdb_drug_map);						
		}
		
		int match_size = 0; int i = 1;
    	for (Iterator<Map.Entry<String, Map<String, Integer>>> iter = pdb_drug_map.entrySet().iterator(); iter.hasNext(); ) 	{
    		Map.Entry<String, Map<String, Integer>> entry = iter.next();
    		match_size += entry.getValue().size();
    		System.out.println(i + " " + entry.getKey() + " " + entry.getValue()); 
    		i++;
    	}
    		
		
		System.out.println("size: " + pdb_drug_map.size() + "    match_size: " + match_size);
		return pdb_drug_map;
	}
	
	private static void add_to_pdb_drug_map(String generic_name, String pdb_id, String pdb_id_similarity, Map<String, Map<String, Integer>> pdb_drug_map) {
		if (pdb_id.length() > 0){ //make sure it is not empty
			Integer similarity = Integer.valueOf(pdb_id_similarity.substring(1, pdb_id_similarity.indexOf("%")));
			if (similarity >= 90){
				if (pdb_drug_map.containsKey(pdb_id)){
					Map<String, Integer> drugMap = pdb_drug_map.get(pdb_id);
					if (drugMap.containsKey(generic_name)){
						if (drugMap.get(generic_name) < similarity)
							drugMap.put(generic_name, similarity);
					}else{
						drugMap.put(generic_name, similarity);
					}
				}else{
					Map<String, Integer> drugMap = new HashMap<String, Integer>();
					drugMap.put(generic_name, similarity);
					pdb_drug_map.put(pdb_id, drugMap);
				}						
			}					
		}		
	}

}
