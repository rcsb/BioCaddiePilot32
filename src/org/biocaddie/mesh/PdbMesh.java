package org.biocaddie.mesh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector; 

/**
 * We started this class to analyze mesh network, but it is not complete, currently we only read meshData, and generate the network in Pajek format.
 * Also we modified infoMap C++ to take this Pajek net file and generate .bftree file, so we can visualize meshTree using the infoMap "Hierarchical Network Navigator" http://www.mapequation.org/apps/NetworkNavigator.html
 * TODO: We need to clean up this class when we have time later... First we need to work on citation networks...
 */
public class PdbMesh {
    
	final static Charset ENCODING = StandardCharsets.UTF_8;
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static void main(String[] args) throws Exception{		   	
    	   	  
    	String pathToFile = "/Users/ali/Documents/BioCaddie/data/citation/april20/";
    	 
    	//****this code is about MESH data analysis, not citation analysis, need to move to another package.
		//Step 1: Read MeSH tree file, and generate these three maps as output
/*    	Map<String, MeshNode> meshNodeMap = new HashMap<String, MeshNode>(); //key:UI, value:meshNode object
		Map<String, MeshLink> meshLinkMap = new HashMap<String, MeshLink>(); //key:MN, value:meshLink object
		Map<String, String> allMeshTermsMap = new HashMap<String, String>(); //key:meshTerm, value:meshDescriptorUI		
		readMeshTreeFile("/Users/ali/Documents/BioCaddie/meshData/d2015.bin", meshNodeMap, meshLinkMap, allMeshTermsMap);
*/		
		//Step 2: Read PDB report  
//		Map<String, PDBNode> pdbNodeMap = new HashMap<String, PDBNode>(); //key:MN, value:meshLink object
//		readPDBMeshTermsFile(pathToFile+"pdbDataApril2.csv", pdbNodeMap);	
		   
		//Step 3: Associate PDB ids with MeSH Tree, so we have: MH [MN] --> numOfAssociatedPDBs | numOfAssociatedPDBsCumulative (ex: "Formamides" [D02.065.463] | 20 | 24 )
		//associatePDBdataWithMeshTree(pdbNodeMap, meshNodeMap, meshLinkMap, allMeshTermsMap);
		//Step 4: Generate Pajek net file for infoMap Visualization
		//generatePajekNetFile("mesh.net", meshNodeMap, meshLinkMap);
				
	}
		
	//Read PDB report in CSV format, which has PDB id and its related MeSH terms
	//PDBid - PubYear - PMCid - PMid - MeshTerms
	public static void readPDBMeshTermsFile(String fileName, Map<String, PDBNode> pdbNodeMap) throws IOException{
		Path path = Paths.get(fileName);
		try (BufferedReader reader = Files.newBufferedReader(path, ENCODING)){
			String line = null;
		    while ((line = reader.readLine()) != null) {
		    	//skip the header line and any empty lines
		    	if (line.startsWith("PDB") || line.trim().equals(""))
		    		continue;
		    	String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		    	String pdbId   = tokens[0].replaceAll("\"", "");
		    	String pubYear = tokens[1].replaceAll("\"", "");
		    	String pmcId   = tokens[2].replaceAll("\"", "");
		    	String pmId    = tokens[3].replaceAll("\"", "");
		    	String meshTs  = tokens[4].replaceAll("\"", "");
		    	
		    	Vector<String> meshTerms = new Vector<String>();
		    	if (meshTs.length() > 0){
		    		String[] meshTermsArray = meshTs.split(", ");		    			    	      
		    		Collections.addAll(meshTerms, meshTermsArray);
		    	}
		    	PDBNode pdbNode = new PDBNode(pdbId, pubYear, pmcId, pmId, meshTerms);
		    	pdbNodeMap.put(pdbId, pdbNode);
		    }
		}catch (Exception e){
			e.printStackTrace();
		}		  		
	}
	
	//read ASCII MeSH Tree file (d2015.bin) as input, and generates meshNodeMap(descriptors) and meshLinkMap(MN's) as output
	public static void readMeshTreeFile(String fileName, Map<String, MeshNode> meshNodeMap, Map<String, MeshLink> meshLinkMap, Map<String, String> allMeshTermsMap) throws IOException{
		
		Path path = Paths.get(fileName);
		try (BufferedReader reader = Files.newBufferedReader(path, ENCODING)) { 			
			
			//Map<String, MeshNode> meshNodeMap = new HashMap<String, MeshNode>(); 			
		    //add a single ROOT, where all sciences meets
			int cnt_meshNode_id = 1;
	    	meshNodeMap.put("0", new MeshNode("ROOT-SCIENCE", "0", "U00", String.valueOf(cnt_meshNode_id)));
		   	cnt_meshNode_id++;
		    
		    //root-level categories don't exist in the file, so we manually add them
		    meshNodeMap.put("A", new MeshNode("Anatomy", "A", "U01", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++; 
		    meshNodeMap.put("B", new MeshNode("Organisms", "B", "U02", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("C", new MeshNode("Diseases", "C", "U03", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("D", new MeshNode("Chemicals and Drugs", "D", "U04", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("E", new MeshNode("Analytical, Diagnostic and Therapeutic Techniques and Equipment", "E", "U05", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("F", new MeshNode("Psychiatry and Psychology", "F", "U06", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("G", new MeshNode("Phenomena and Processes", "G", "U07", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("H", new MeshNode("Disciplines and Occupations", "H", "U08", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("I", new MeshNode("Anthropology, Education, Sociology and Social Phenomena", "I", "U09", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("J", new MeshNode("Technology, Industry, Agriculture", "J", "U010", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("K", new MeshNode("Humanities", "K", "U011", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("L", new MeshNode("Information Science", "L", "U012", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("M", new MeshNode("Named Groups", "M", "U013", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("N", new MeshNode("Health Care", "N", "U014", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("V", new MeshNode("Publication Characteristics", "V", "U015", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    meshNodeMap.put("Z", new MeshNode("Geographicals", "Z", "U016", String.valueOf(cnt_meshNode_id))); cnt_meshNode_id++;
		    
		    //general map which stores all mesh terms, and their corresponding meshNodeUI (key:meshTerm, value:meshDescriptorUI)
		    //Map<String, String> allMeshTermsMap = new HashMap<String, String>(); 
		    int cnt_duplicate = 0;
			MeshNode meshNode = new MeshNode();
			String line = null; 
		    while ((line = reader.readLine()) != null) {
		    	
		    	if (line.startsWith("*NEWRECORD"))// create new node
		    		meshNode= new MeshNode();
		    		
		    	if (line.startsWith("MH = ")){ //store MH(Main Heading),
		    		meshNode.MH = line.substring(5);
		    		meshNode.terms.add(meshNode.MH);// add MH to the terms.
		    	}
		    	
		    	if (line.startsWith("MN = ")) //MN(MeshTree Number)s, may be more than one for each record
		    		meshNode.MNlist.add(line.substring(5));
		    	
		    	if (line.startsWith("ENTRY = ")){ //MeSH terms, may be more than one for each record
		    		String entry = line.substring(8);
		    		if (entry.contains("|"))
		    			meshNode.terms.add(entry.substring(0, entry.indexOf("|")));
		    		//else
		    		//	meshNode.terms.add(entry);
		    	}
		    		
		    	if (line.startsWith("UI = ")){ //since UniqueIdentifier is the last element for a record, add the descriptor to the meshNodeMap
		    		meshNode.UI = line.substring(5);
		    		if (!meshNode.MH.equals("Female") && !meshNode.MH.equals("Male")){ //exclude these two weird records which don't have MNs
		    			meshNode.node_id =  String.valueOf(cnt_meshNode_id); 
		    			cnt_meshNode_id++;
		    			meshNodeMap.put(meshNode.UI, meshNode);
		    			
		    			//now we have meshNode.UI, we will add meshTerms of this node to the GlobalMeshTermsMap
		    			for (Iterator<String> iter = meshNode.terms.iterator(); iter.hasNext(); ) {
		    				String term = iter.next();
		    				if (allMeshTermsMap.containsKey(term)){
		    					System.out.println("key exist"+term+" "+meshNode.UI);
		    					cnt_duplicate++;
		    				}
		    				allMeshTermsMap.put(term, meshNode.UI);		    				
		    			}
		    		}
		    	}		    		
		    }
		    System.out.println("meshNodeSize: " + meshNodeMap.size());
		    System.out.println("meshTermsMapSize: " + allMeshTermsMap.size());
		    
		    //sort the meshNodeMap based on keys
		//    Map<String, MeshNode> sortedMap = new TreeMap<String, MeshNode>(meshNodeMap);
		 //   meshNodeMap = sortedMap;

		    //Fill the links Map
		    int cnt_meshLink_id = 1;
			//Map<String, MeshLink> meshLinkMap = new HashMap<String, MeshLink>(); 
		    for (Iterator<Map.Entry<String, MeshNode>> iter = meshNodeMap.entrySet().iterator(); iter.hasNext(); ) {
		    	
		    	Map.Entry<String, MeshNode> entry = iter.next();	
		    	String meshNode_UI = entry.getKey();
		    	for (Iterator<String> iter2 = entry.getValue().MNlist.iterator(); iter2.hasNext(); ) {
		    		
		    		String self_MN_value = iter2.next();
		    		String parent_MN_value = "";
		    		
			    	if (self_MN_value.equals("0")){
			    		//Case 0: if it is the Single ROOT-SCIENCE, it does not have parent, 			    		
			    		parent_MN_value = "";
			    	}else if (self_MN_value.length() == 1 && !self_MN_value.equals("0")){ 
			    		//Case 1: if the length of MN=1 and notEquals(0) then it is a main category (A, B, C, etc.)
				    	//it doesn't have a parent, so set its parent to 0
			    		parent_MN_value = "0"; 		    	   
				    }else if (self_MN_value.length() > 1 && !self_MN_value.contains(".")){ 
				    	//Case 2: if the length of MN>1 and MN does not contain dot(.), then it is a subcategory (A01, B02, etc.)
				    	//the first letter of the MN = MN of the parent (A01 => A)
				    	parent_MN_value = self_MN_value.substring(0, 1);
				    }else { 
				    	//Case 3: for all other cases, there should be dot(.) within the MN
				    	//substring of the MN before the last dot(.) = MN of the parent (H01.770.644.053 => H01.770.644)
				    	parent_MN_value = self_MN_value.substring(0, self_MN_value.lastIndexOf("."));
				    }
			    	
			    	meshLinkMap.put(self_MN_value, new MeshLink(self_MN_value, parent_MN_value, meshNode_UI, String.valueOf(cnt_meshLink_id) ));
			    	cnt_meshLink_id ++;
		    	}
		    }
    
		    System.out.println("meshLinkSize: " + meshLinkMap.size());		   	    		    
		}catch (Exception e){
			e.printStackTrace();
		}	
	}	

	//Step 3: Associate PDB ids with MeSH Tree, so we have: MH [MN] --> numOfAssociatedPDBs | numOfAssociatedPDBsCumulative (ex: "Formamides" [D02.065.463] | 20 | 24 )
	public static void associatePDBdataWithMeshTree(Map<String, PDBNode> pdbNodeMap, Map<String, MeshNode> meshNodeMap, Map<String, MeshLink> meshLinkMap, Map<String, String> allMeshTermsMap) {
		//find the numberof meshTerms that doesnot exist in the allMeshTermsMap
		int cntNotFound = 0; int cntFound =0;
	    for (Iterator<Map.Entry<String, PDBNode>> iter = pdbNodeMap.entrySet().iterator(); iter.hasNext(); ) {	    	
	    	Map.Entry<String, PDBNode> entry = iter.next();	    	
	    	boolean flag = false;
			for (Iterator<String> iter2 = entry.getValue().meshTerms.iterator(); iter2.hasNext(); ) {
				String term = iter2.next();
				if (allMeshTermsMap.containsKey(term)){	
					MeshNode mNode = meshNodeMap.get(allMeshTermsMap.get(term));
					mNode.pdbMap.put(entry.getValue().pdbId, entry.getValue().pdbId);
					cntFound++;
					flag = true;
				}else{
					cntNotFound++;
				}
			}
	    }
				
		//find child MNs (Each MNlink has one parent, many children)
	    for (Iterator<Map.Entry<String, MeshLink>> iter = meshLinkMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Map.Entry<String, MeshLink> entry = iter.next();
	    	if (entry.getKey().equals("0"))
	    		continue;
	    	meshLinkMap.get(entry.getValue().parentMN).childList.add(entry.getValue().MN);
	    	//copy the pdbMap from meshNode to meshLink, initially pdbMap = pdbMapCumulative 
	    	entry.getValue().pdbMap.putAll(meshNodeMap.get(entry.getValue().meshNodeUI).pdbMap);
	    	entry.getValue().pdbMapCumulative.putAll(meshNodeMap.get(entry.getValue().meshNodeUI).pdbMap);
	    }

	    //set sumOfChildren for each node
	    for (Iterator<Map.Entry<String, MeshLink>> iter = meshLinkMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Map.Entry<String, MeshLink> entry = iter.next();
	    	entry.getValue().sumOfChildren = entry.getValue().childList.size();		    	
	    }
	    
	    for (Iterator<Map.Entry<String, MeshLink>> iter = meshLinkMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Map.Entry<String, MeshLink> entry = iter.next();
	    		MeshLink parent = meshLinkMap.get(entry.getValue().parentMN);
	    		while (parent != null){
	    			parent.sumOfChildrenCumulative++; //= parent.sumOfChildrenCumulative + cumulativeChildCnt;
	    			
	    			parent.pdbMapCumulative.putAll(entry.getValue().pdbMap);
	    			
	    			parent = meshLinkMap.get(parent.parentMN);	    				    			
	    		}		    				    				    
	    }		    
	}

    //Generate Pajek net file
	public static void generatePajekNetFile(String fileName, Map<String, MeshNode> meshNodeMap, Map<String, MeshLink> meshLinkMap) throws IOException {

		File file = new File(fileName);
		FileOutputStream fos = new FileOutputStream(file);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		bw.write("*Vertices " + meshLinkMap.size()); bw.newLine();
	    for (Iterator<Map.Entry<String, MeshLink>> iter = meshLinkMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Map.Entry<String, MeshLink> entry = iter.next();
	    	//if (entry.getValue().parentMN.equals("0")){
	    	bw.write(entry.getValue().link_id + " \"" + meshNodeMap.get(entry.getValue().meshNodeUI).MH + "\" [" +entry.getValue().MN +"]"+ " | "+ entry.getValue().pdbMap.size() +" | "+ entry.getValue().pdbMapCumulative.size() ); 
	    	bw.newLine();
	    	//}
	    }
	    
	    bw.write("*Arcs " + (meshLinkMap.size()-1)); bw.newLine();
	    for (Iterator<Map.Entry<String, MeshLink>> iter = meshLinkMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Map.Entry<String, MeshLink> entry = iter.next();
	    	//root doesn't have parent, skip it 
	    	if (entry.getKey().equals("0"))
	    			continue;

	    	String from = entry.getValue().link_id;
	    	String to = meshLinkMap.get(entry.getValue().parentMN).link_id;
	    	String weight = "1.0"; //default weight, currently we don't have weight for links
	    	
	    	bw.write(from + " " + to + " " + weight); bw.newLine();
	    } 	    
		bw.close();		   	
	}	


}//end of class 


//A utility class to store MeSH descriptors, only the fields we need
class MeshNode{
	  MeshNode(){};
	  MeshNode(String mh, String mn, String ui, String id){
		  MH = mh;
		  if (!mn.equals(""))
			  MNlist.add(mn);
		  UI = ui;
		  node_id = id;
	  };
	  
	  String MH; //Main Heading, different descriptors(UIs) may have the exact same MH
	  Vector<String> MNlist = new Vector<String>(); //a list to store MNs of this node       
	  String UI; //Unique Identifier	  
	  Vector<String> terms = new Vector<String>();	  

	  //this map stores the list of pdbIDs which uses at least one term of this mesh descriptor, 
	  Map<String, String> pdbMap = new HashMap<String, String>(); //key:pdbID, value:pdbID

	  String node_id; //we generate this unique id starting from 1, used for .net file
};

//A utility class to store MN links
class MeshLink{
	  MeshLink(){};
	  MeshLink(String mn, String parent_mn, String meshNode_ui, String id){
		  MN = mn;
		  parentMN = parent_mn;
		  meshNodeUI = meshNode_ui;
		  link_id = id;
	  };
	  
	  String MN;
	  String parentMN;
	  String meshNodeUI; // to access the MeshNode
	  String link_id; //we generate this unique id starting from 1, used for .net file
	  
	  Vector<String> childList = new Vector<String>(); //a list to store MNs of its children       

	  Map<String, String> pdbMap = new HashMap<String, String>(); //key:pdbID, value:pdbID
	  Map<String, String> pdbMapCumulative = new HashMap<String, String>(); //key:pdbID, value:pdbID

	  //int level; //the level of the link, ROOT-level = 0, categories = 1, etc
	  int sumOfChildren = 0;
	  int sumOfChildrenCumulative = 0;
} 

class PDBNode{
	PDBNode(){};
	PDBNode(String p_pdbId, String p_pubYear, String p_pmcId, String p_pmId, Vector<String> p_meshTerms){
		pdbId = p_pdbId;
		pubYear = p_pubYear;
		pmcId = p_pmcId;
		pmId = p_pmId;
		meshTerms = p_meshTerms;
	};
	  
	String pdbId;	
	String pubYear;
	String pmcId;
	String pmId;
	Vector<String> meshTerms = new Vector<String>();      
};

