package org.biocaddie.citationanalysis.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FileUtility {
    final static Charset ENCODING = StandardCharsets.UTF_8;

	public static void main(String[] args) throws Exception {
	
		String inputTreeFile = "/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/community_detection/community_detection_algorithms/Infomap/output_cocitation/2014_paper_citation_network_cocitation.tree";
		String outputMapFile = "/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/community_detection/community_detection_algorithms/Infomap/output_cocitation/2014_paper_citation_network_cocitation.map";
		convertInfomapTreefileToMapfile(inputTreeFile, outputMapFile);

		System.out.println("Done...");
	}
	
	/**
	 * Some large tree files(1M node network) cannot be visualized using infoMap Flash Player due to timeout. 
	 * Because of that we generate map file for the highest level modules from the .tree file. So visualize large network.
	 * @throws Exception 
	 */
	private static void convertInfomapTreefileToMapfile(String inputTreeFile, String outputMapFile) throws Exception{
		
		Map<Integer, Double> newMap = new HashMap<Integer, Double>(); //moduleId, moduleSize
		
	    BufferedReader reader = Files.newBufferedReader(Paths.get(inputTreeFile), ENCODING);
		String line = null; 
	    while ((line = reader.readLine()) != null) {	    	
	    	line = line.trim();
	    	if (line.startsWith("#") || line.equals("")) //skip the first line and empty lines, if there is any
	    		continue;
	    	
	    	int firstApostropheIndex = line.indexOf("\"");
	    	int firstColonIndex = line.indexOf(":");
	    	int firstSpaceIndex = line.indexOf(" ");
	    	
	    	Integer moduleId = Integer.valueOf(line.substring(0, firstColonIndex)); 
	    	Double size = Double.valueOf(line.substring(firstSpaceIndex, firstApostropheIndex).trim());
	    	
	    	if (newMap.containsKey(moduleId))
	    		newMap.put(moduleId, newMap.get(moduleId)+size);
	    	else
	    		newMap.put(moduleId, size);
	    }
	    
	    
	    //
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(outputMapFile)));
		out.write("*Undirected"); out.newLine();
		out.write("*Modules "+newMap.size()); out.newLine();
		for (Iterator<Map.Entry<Integer, Double>> iter = newMap.entrySet().iterator(); iter.hasNext(); ) {
			Map.Entry<Integer, Double> e = iter.next();
			out.write(e.getKey() + " \""+e.getKey()+"\" " +e.getValue()); out.newLine();
		}
		

	    System.out.println("test" + newMap.size());
		
	}
	

}
