package org.biocaddie.citationanalysis.retrievedata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Starting from the list of unique PDB primary citations (PubMed Ids), this class retrieves the citations links from PubMed via eutils/elink service in a cascading style.
 * We download PDB data and their associated PrimaryCitation PubMedIds in csv format, from http://www.rcsb.org/pdb/home/home.do and click number "107251" -> reports -> customizable data -> primary citation. 
 * PDB ID,PubMed ID
 * "100D","7816639"
 * "101D","7711020"
 * "101M",""
 * "102D","7608897"
 * ..............
 * Then, we retrieve the citationLinks data from NCBI E-utils using HttpRequest. 
 * Since we are retrieving large data, we registered the project and sent the registered name and our email address via HttpRequest "$\&tool=BioCADDIE\&email=abc@ucsd.edu$". 
 * We retrieve the data in a batch mode, each HttpRequest retrieves XML file for 10K PubMed Ids or less for the last one. 
 * In order to make sure that each XML file is received completely, we checked whether the Http Response (Code = 200) is successful, and the endTag is correct. 
 * If there is a problem, the program prints the error message and exits.
 * Sample http request: 
 * http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?tool=BioCADDIE&email=a@ucsd.edu&retmode=xml&dbfrom=pubmed&cmd=neighbor&linkname=pubmed_pubmed_citedin&id=2344535&id=1211543&.......
 * 
 * This program generates the following files as output:
 * - one xml file for each cascade_level (citations_level_x.xml)
 * - single xml file which contains the citations of all cascade_levels (all_citations.xml)
 * - single txt file which contains the list of all retrieved pubmed ids.(all_pubmed_id.txt)
 * - all_statistics.txt file which contains the statistics about the data retrieval.
 */
public class RetrieveCitationFromEutils {

    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final Map<String, Integer> globalPubMedIdMap = new HashMap<String, Integer>(); //unique pubMedId's for all levels
    private static BufferedWriter out_all_citations;
    private static BufferedWriter out_statistics;
	
    /**
     * The main function. 
     * @param args: It only accepts one input argument: pdbId_pubMedId.csv file.
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {

	//Get the inputFileName from user 
		if(args.length != 1){
    		System.out.println("Call: java org.biocaddie.citation.retrievedata.RetrieveCitationFromEutils <PdbId_PubMedId.csv>");
    		System.exit(1);
    	}
		String fileNameFullPath = args[0];		
		String fileSeparator = System.getProperty("file.separator");		
		String pathToFile = fileNameFullPath.substring(0, fileNameFullPath.lastIndexOf(fileSeparator)+1);		
    	
    	//output files
		out_all_citations = new BufferedWriter(new FileWriter(new File(pathToFile + "all_citations.xml")));
		out_statistics = new BufferedWriter(new FileWriter(new File(pathToFile + "all_statistics.txt")));

    	//STEP 1: Read the input file and generate pubmed_id_map which contains the unique PMids of primary citations. (First-level)
		Map<String, Integer> pubmed_id_map = new HashMap<String, Integer>(); //unique pubMed Id's (key: pubMedId value: count)		
    	readInputFile(fileNameFullPath, pubmed_id_map);
    	    	
	    //STEP 2: Starting from the first-level(unique primary citations), iteratively navigate the citations level, until we have the all pubMed IDs in our globalPmIdMap
    	//For each level, first we retrieve the citation cascades (forward in time), then we retrieve the reference cascades (backward in time), until we have the whole network
    	int cascadeLevel = 1;	    
	    while (pubmed_id_map.size() > 0){
	    	pubmed_id_map = getPubMedCitations(pathToFile+"citations_level_"+cascadeLevel+".xml", pubmed_id_map, cascadeLevel);
	    	//pubmed_id_map = getPubMedCitations(pathToFile+"references_level_"+cascadeLevel+".xml", pubmed_id_map, cascadeLevel);
	    	cascadeLevel++;
	    }
	    
	    //STEP 3: Write globalPubMedIdMap to a file (the list of all unique pmIds in our network)
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(pathToFile+"all_pubmed_id.txt")));
        out.write("*PubMed Id | Count)  size: " + globalPubMedIdMap.size());
        out.newLine();
	    for (Iterator<Map.Entry<String, Integer>> iter = globalPubMedIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();
	        out.write(entry.getKey() + " | " + entry.getValue());
	        out.newLine();
	    }
        out.flush();       
        out.close();    
	    
		System.out.println("DONE !!!");	out_statistics.write("DONE !!!");
		out_statistics.flush();
		out_statistics.close();				
		
		
	//	getWholePubMedCitations();
	}

	/**
	 * Read the input file which contains the PDB id's and their primary citations into pubmed_id_map.
	 * @param inputFileName: the input file in csv format
	 * @param pubmed_id_map: reads the input file and puts the pubmed ids to this HashMap. (key: pubMedId value: count)
	 * @throws IOException
	 */
	public static synchronized void readInputFile(String inputFileName, Map<String, Integer> pubmed_id_map) throws IOException {
		
    	BufferedReader reader = Files.newBufferedReader(Paths.get(inputFileName), ENCODING);
		int cnt_pdb_id = 0; //Number of PDB id within the input file
    	int cnt_primary_citation = 0; //Number of PDB id with a primary citation
    	String line = null;
		while ((line = reader.readLine()) != null) {			
			//skip the header line and any empty lines
			if (line.trim().startsWith("PDB") || line.trim().equals(""))
				continue;
			cnt_pdb_id++;
			
			String[] tokens  = line.split(","); 
		    String pdb_id    = tokens[0].replaceAll("\"", "").trim();
		    String pubmed_id = tokens[1].replaceAll("\"", "").trim();
		    
		    if (pubmed_id.length() > 0){ // if the primary citation PubMed id is not empty, add it to the map.		    	
		    	cnt_primary_citation++;		    			    	
		    	if (pubmed_id_map.containsKey(pubmed_id)){
		    		int currentValue = pubmed_id_map.get(pubmed_id).intValue();
		    		pubmed_id_map.put(pubmed_id, new Integer(currentValue+1));	    		
		    	}else{	    		
		    		pubmed_id_map.put(pubmed_id, new Integer(1));
		    	}	
		    }
		}		
		
	    StringBuffer statistics = new StringBuffer();
	    statistics.append("Start reading: " + inputFileName);statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PDB id: " + cnt_pdb_id);statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PDB id with a primary citation: " + cnt_primary_citation);statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of unique primary citation: " + pubmed_id_map.size());statistics.append(System.getProperty("line.separator"));
	    statistics.append("Will retrieve the citation cascades starting from the " + pubmed_id_map.size() + " unique primary citations (using eutils.pubmed)");statistics.append(System.getProperty("line.separator"));
	    statistics.append("It may take around one hour or more ...");statistics.append(System.getProperty("line.separator"));
	    statistics.append("------------------------------------------------------ "); statistics.append(System.getProperty("line.separator"));
    	System.out.println(statistics.toString()); out_statistics.write(statistics.toString());
    	out_statistics.flush();		
	}

	public static synchronized void getWholePubMedCitations() throws Exception{
		
		//eutils HTTP requests usually aborted after 3 minutes. Because of that, for each level, we divide pubMedIdList into 10K partitions and send HttpRequests for them,
		//but we write the result of each level to a single file. 
		
		//This is a special method, first we read the whole pubMed id from the file
		int level = 1;
		Map<String, Integer> pmIdMap = new HashMap<String, Integer>();
		String newXmlFileName = "/Users/ali/Documents/BioCaddie/data/citation/april_29/whole_pubmed/all_citations.xml";
		
	    BufferedReader reader = Files.newBufferedReader(Paths.get("/Users/ali/Documents/BioCaddie/data/citation/april_29/whole_pubmed/all_pubmed_id.txt"), ENCODING);
		String line = null; 
	    while ((line = reader.readLine()) != null) {
	    	//skip the header line and any empty lines
	    	if (line.startsWith("*") || line.trim().equals("")) //first line or last line
	    		continue;	    	
	    	pmIdMap.put(line.trim(), 0); //default value 0, we don't need value
	    }		

	    //Generate the pmIdList stringBuffer from the HashMap
    	Vector<StringBuffer> requestIdListVector = new Vector<StringBuffer>(); //keep requestLists of 10K size
		StringBuffer requestIdList = new StringBuffer(); 
		int cnt_tmp = 0;
	    for (Iterator<Map.Entry<String, Integer>> iter = pmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    	
	    	Map.Entry<String, Integer> entry = iter.next();	    	
	    	requestIdList.append("&id="); 
	    	requestIdList.append(entry.getKey());
	    	cnt_tmp++;
	    	if (cnt_tmp == 10000){
	    		requestIdListVector.add(requestIdList);
	    		requestIdList = new StringBuffer(); 
	    		cnt_tmp = 0;
	    	}	    	
	    }
	    //add the last one too, if it is not empty
	    if (requestIdList.length() > 0){
    		requestIdListVector.add(requestIdList);
    		requestIdList = new StringBuffer(); 
    		cnt_tmp = 0;
	    }
	    
		Map<String, Integer> newPmIdMap = new HashMap<String, Integer>(); //new unique pmId's
		//Map<String, Integer> newPmIdMapBackup = new HashMap<String, Integer>(); //clone, we need this clone when an exception occurs
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(newXmlFileName)));
	    
	    String str_level = "CASCADE LEVEL: " + level; System.out.println(str_level);                      //out_statistics.write(str_level); out_statistics.newLine();
	    String startTime = "Start Time: " + dateFormat.format(new Date()); System.out.println(startTime); //out_statistics.write(startTime); out_statistics.newLine();
	    //out_statistics.flush();
		
		String elink_url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi";
	    String toolEmailLinkname ="tool=BioCADDIE&email=altunkay@hawaii.edu&retmode=xml&dbfrom=pubmed&cmd=neighbor&linkname=pubmed_pubmed_citedin,pubmed_pubmed_refs";
	    //String toolEmailLinkname ="tool=BioCADDIE&email=altunkay@hawaii.edu&retmode=xml&dbfrom=pubmed&cmd=neighbor&linkname=pubmed_pubmed_refs";
	    
	    //iterate through the requestIdListVector
	    for (int i = 0; i < requestIdListVector.size(); i++){
	    	boolean firstRequestList = false; 
	    	boolean lastRequestList = false;
	    	if (i == 0)
	    		firstRequestList = true;
	    	if (i == (requestIdListVector.size()-1))
	    		lastRequestList = true;
	    	
	    	String elink_urlParameters = toolEmailLinkname + requestIdListVector.get(i).toString();
			StringBuffer outStringBuffer = new StringBuffer(); // this is for out file		
			StringBuffer outAllStringBuffer = new StringBuffer(); // this is for out_all_citations.xml
	    	
			
			try {
				eutilsHttpRequest(firstRequestList, lastRequestList, elink_url, elink_urlParameters, level, newPmIdMap, outStringBuffer, outAllStringBuffer);
				out.write(outStringBuffer.toString());	out.flush();  
			} catch (Exception e) {
				//If there is an exception such as "SocketException", connection problem or truncated XML, wait 5 minutes and retry. 
				//If the error appears again after 2 tries, don't catch it again, so the program will exit.
				String errorMessage = e.getMessage(); 
				String currentTime = dateFormat.format(new Date());
			    System.out.println("Exception: " + errorMessage + " currentTime:" + currentTime);

			    Thread.sleep(300000); // wait 5 minutes,  		  
			    
			    //initialize variables			    
			    outStringBuffer = new StringBuffer(); 
			    outAllStringBuffer = new StringBuffer(); 
			    
				try {
					eutilsHttpRequest(firstRequestList, lastRequestList, elink_url, elink_urlParameters, level, newPmIdMap, outStringBuffer, outAllStringBuffer);
					out.write(outStringBuffer.toString());	out.flush();
				} catch (Exception e1) {				
					errorMessage = e1.getMessage(); 
					currentTime = dateFormat.format(new Date());
				    System.out.println("Exception: " + errorMessage + " currentTime:" + currentTime);

				    Thread.sleep(300000); // wait 5 minutes,  		  
				    
				    //initialize variables			    
				    outStringBuffer = new StringBuffer(); 
				    outAllStringBuffer = new StringBuffer(); 

					eutilsHttpRequest(firstRequestList, lastRequestList, elink_url, elink_urlParameters, level, newPmIdMap, outStringBuffer, outAllStringBuffer);
					out.write(outStringBuffer.toString());	out.flush();				    
				}  
				
			}
	    }
	    
        //flush and close the outstream
        out.flush();  //out_all_citations.flush();  
        out.close();        
		
	    String endTime = "End Time  : " + dateFormat.format(new Date());
        System.out.println(endTime);  //out_statistics.write(endTime); out_statistics.newLine(); out_statistics.flush();

	    //Now, we have completed all HTTP requests for this cascade level
	    //write the received pmIDs to the globalMap, we are sure that they don't exist in the globalPmIdMap, it is checked in the previous level
/*		int pmTotal = 0;
	    for (Iterator<Map.Entry<String, Integer>> iter = pmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();	    	
	    	globalPubMedIdMap.put(entry.getKey(), entry.getValue());
	    	pmTotal = pmTotal + entry.getValue();
	    }		
		
	    //If the new citations are already exist in the globalPmIdMap, do not need to request them again.
	    int newPmTotal = 0; int newPmNotExistTotal = 0;
		Map<String, Integer> newPmIdMapNotExist = new HashMap<String, Integer>(); //new unique pmId's
	    for (Iterator<Map.Entry<String, Integer>> iter = newPmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();	   
	    	newPmTotal = newPmTotal + entry.getValue();
	    	if (globalPubMedIdMap.containsKey(entry.getKey())){
	    		int currentValue = globalPubMedIdMap.get(entry.getKey()).intValue();
	    		globalPubMedIdMap.put(entry.getKey(), new Integer(currentValue + entry.getValue().intValue()));
	    	}else{
	    		newPmIdMapNotExist.put(entry.getKey(), entry.getValue());
	    		newPmNotExistTotal = newPmNotExistTotal + entry.getValue();
	    	}
	    }
	    
	    //if this is the last cascade level (newPmIdMapNotExist.size() == 0) then append the endTag and flush/close the all_citations.xml
	    if (newPmIdMapNotExist.size() == 0){
	    	out_all_citations.write("</eLinkResult>");
			out_all_citations.newLine();					
	    	out_all_citations.flush();
	    	out_all_citations.close();
	    }
	    
	    //Print statistics of the current level to the both command prompt and statistics.txt file
	    StringBuffer statistics = new StringBuffer();     
	    statistics.append("Statistics: ");    statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PM IDs retrieved at this level: " + pmIdMap.size());statistics.append(System.getProperty("line.separator")); 
	    statistics.append("PM ids (Total | Unique): " + pmTotal + " | " + pmIdMap.size());statistics.append(System.getProperty("line.separator"));
	    statistics.append("newPM Ids or Citations (Total | Unique): " + newPmTotal + " | " + newPmIdMap.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("newPM Ids or Citations - Not ExistBefore (Total | Unique): " + newPmNotExistTotal +  " | " + newPmIdMapNotExist.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PM IDs will be retrieved at the next level: " + newPmIdMapNotExist.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("Size of global PM IDs at this level: " + globalPubMedIdMap.size()); statistics.append(System.getProperty("line.separator"));    
	    statistics.append("------------------------------------------------------ "); statistics.append(System.getProperty("line.separator"));
    	System.out.println(statistics.toString()); out_statistics.write(statistics.toString());
    	out_statistics.flush();
	    
	    return newPmIdMapNotExist;
*/	    
	}	
	
	/**
	 * get pubMed citations of each pmId in the "pmIdList" and write all of them into a new XML file ("newXmlFileName")
	 * generate summary (total # of pmIDs, unique # of pmIDs, # of new unique pmIDs in the all of the citations in this level)
	 * @param newXmlFileName
	 * @param pmIdMap : list of pubmed id's that will be retrieved in this cascade_level
	 * @param level : the cascade level of citations
	 * @return newPmIdMapNotExist: the list of new pubMed id's that we encountered in this cascade_level, and does not exist in our global_list, 
	 * therefore we will retrieve them in the next cascade level
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static synchronized Map<String, Integer> getPubMedCitations(String newXmlFileName, Map<String, Integer> pmIdMap, int level) throws Exception{
		
		//eutils HTTP requests usually aborted after 3 minutes. Because of that, for each level, we divide pubMedIdList into 10K partitions and send HttpRequests for them,
		//but we write the result of each level to a single file. 
		
		//Generate the pmIdList stringBuffer from the HashMap
    	Vector<StringBuffer> requestIdListVector = new Vector<StringBuffer>(); //keep requestLists of 10K size
		StringBuffer requestIdList = new StringBuffer(); 
		int cnt_tmp = 0;
	    for (Iterator<Map.Entry<String, Integer>> iter = pmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    	
	    	Map.Entry<String, Integer> entry = iter.next();	    	
	    	requestIdList.append("&id="); 
	    	requestIdList.append(entry.getKey());
	    	cnt_tmp++;
	    	if (cnt_tmp == 10000){
	    		requestIdListVector.add(requestIdList);
	    		requestIdList = new StringBuffer(); 
	    		cnt_tmp = 0;
	    	}	    	
	    }
	    //add the last one too, if it is not empty
	    if (requestIdList.length() > 0){
    		requestIdListVector.add(requestIdList);
    		requestIdList = new StringBuffer(); 
    		cnt_tmp = 0;
	    }
	    
		Map<String, Integer> newPmIdMap = new HashMap<String, Integer>(); //new unique pmId's
		Map<String, Integer> newPmIdMapBackup = new HashMap<String, Integer>(); //clone, we need this clone when an exception occurs
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(newXmlFileName)));
	    
	    String str_level = "CASCADE LEVEL: " + level; System.out.println(str_level);                      out_statistics.write(str_level); out_statistics.newLine();
	    String startTime = "Start Time: " + dateFormat.format(new Date()); System.out.println(startTime); out_statistics.write(startTime); out_statistics.newLine();
	    out_statistics.flush();
		
		String elink_url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi";
	    String toolEmailLinkname ="tool=BioCADDIE&email=altunkay@hawaii.edu&retmode=xml&dbfrom=pubmed&cmd=neighbor&linkname=pubmed_pubmed_citedin,pubmed_pubmed_refs";
	    //String toolEmailLinkname ="tool=BioCADDIE&email=altunkay@hawaii.edu&retmode=xml&dbfrom=pubmed&cmd=neighbor&linkname=pubmed_pubmed_refs";
	    
	    //iterate through the requestIdListVector
	    for (int i = 0; i < requestIdListVector.size(); i++){
	    	boolean firstRequestList = false; 
	    	boolean lastRequestList = false;
	    	if (i == 0)
	    		firstRequestList = true;
	    	if (i == (requestIdListVector.size()-1))
	    		lastRequestList = true;
	    	
	    	String elink_urlParameters = toolEmailLinkname + requestIdListVector.get(i).toString();
			StringBuffer outStringBuffer = new StringBuffer(); // this is for out file		
			StringBuffer outAllStringBuffer = new StringBuffer(); // this is for out_all_citations.xml
	    	
			
			try {
				newPmIdMapBackup = new HashMap<String, Integer>(newPmIdMap); //backup the current newPmIdMap
				eutilsHttpRequest(firstRequestList, lastRequestList, elink_url, elink_urlParameters, level, newPmIdMap, outStringBuffer, outAllStringBuffer);
				out.write(outStringBuffer.toString());	out.flush();  
				out_all_citations.write(outAllStringBuffer.toString()); out_all_citations.flush();
			} catch (Exception e) {
				//If there is an exception such as "SocketException", connection problem or truncated XML, wait 10 seconds and retry. 
				//If the error appears again, don't catch it again, so the program will exit.
				String errorMessage = e.getMessage(); 
				String currentTime = dateFormat.format(new Date());
			    System.out.println("Exception: " + errorMessage + " currentTime:" + currentTime);
			    out_statistics.write("Exception: " + errorMessage + " currentTime:" + currentTime); out_statistics.newLine(); out_statistics.flush();

			    Thread.sleep(10000);  		  
			    
			    //initialize variables			    
			    outStringBuffer = new StringBuffer(); 
			    outAllStringBuffer = new StringBuffer(); 
			    newPmIdMap = new HashMap<String, Integer>(newPmIdMapBackup); //if there is an exception, restore the current newPmIdMapBackup
			    
				eutilsHttpRequest(firstRequestList, lastRequestList, elink_url, elink_urlParameters, level, newPmIdMap, outStringBuffer, outAllStringBuffer);
				out.write(outStringBuffer.toString());	out.flush();  
				out_all_citations.write(outAllStringBuffer.toString()); out_all_citations.flush();
			}
	    }
	    
        //flush and close the outstream
        out.flush();  out_all_citations.flush();  
        out.close();        
		
	    String endTime = "End Time  : " + dateFormat.format(new Date());
        System.out.println(endTime);  out_statistics.write(endTime); out_statistics.newLine(); out_statistics.flush();

	    //Now, we have completed all HTTP requests for this cascade level
	    //write the received pmIDs to the globalMap, we are sure that they don't exist in the globalPmIdMap, it is checked in the previous level
		int pmTotal = 0;
	    for (Iterator<Map.Entry<String, Integer>> iter = pmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();	    	
	    	globalPubMedIdMap.put(entry.getKey(), entry.getValue());
	    	pmTotal = pmTotal + entry.getValue();
	    }		
		
	    //If the new citations are already exist in the globalPmIdMap, do not need to request them again.
	    int newPmTotal = 0; int newPmNotExistTotal = 0;
		Map<String, Integer> newPmIdMapNotExist = new HashMap<String, Integer>(); //new unique pmId's
	    for (Iterator<Map.Entry<String, Integer>> iter = newPmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();	   
	    	newPmTotal = newPmTotal + entry.getValue();
	    	if (globalPubMedIdMap.containsKey(entry.getKey())){
	    		int currentValue = globalPubMedIdMap.get(entry.getKey()).intValue();
	    		globalPubMedIdMap.put(entry.getKey(), new Integer(currentValue + entry.getValue().intValue()));
	    	}else{
	    		newPmIdMapNotExist.put(entry.getKey(), entry.getValue());
	    		newPmNotExistTotal = newPmNotExistTotal + entry.getValue();
	    	}
	    }
	    
	    //if this is the last cascade level (newPmIdMapNotExist.size() == 0) then append the endTag and flush/close the all_citations.xml
	    if (newPmIdMapNotExist.size() == 0){
	    	out_all_citations.write("</eLinkResult>");
			out_all_citations.newLine();					
	    	out_all_citations.flush();
	    	out_all_citations.close();
	    }
	    
	    //Print statistics of the current level to the both command prompt and statistics.txt file
	    StringBuffer statistics = new StringBuffer();     
	    statistics.append("Statistics: ");    statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PM IDs retrieved at this level: " + pmIdMap.size());statistics.append(System.getProperty("line.separator")); 
	    statistics.append("PM ids (Total | Unique): " + pmTotal + " | " + pmIdMap.size());statistics.append(System.getProperty("line.separator"));
	    statistics.append("newPM Ids or Citations (Total | Unique): " + newPmTotal + " | " + newPmIdMap.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("newPM Ids or Citations - Not ExistBefore (Total | Unique): " + newPmNotExistTotal +  " | " + newPmIdMapNotExist.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("Number of PM IDs will be retrieved at the next level: " + newPmIdMapNotExist.size()); statistics.append(System.getProperty("line.separator"));
	    statistics.append("Size of global PM IDs at this level: " + globalPubMedIdMap.size()); statistics.append(System.getProperty("line.separator"));    
	    statistics.append("------------------------------------------------------ "); statistics.append(System.getProperty("line.separator"));
    	System.out.println(statistics.toString()); out_statistics.write(statistics.toString());
    	out_statistics.flush();
	    
	    return newPmIdMapNotExist;
	}	

	public static synchronized void eutilsHttpRequest(boolean firstRequestList, boolean lastRequestList, String elink_url, String elink_urlParameters, int level, Map<String, Integer> newPmIdMap, StringBuffer outStringBuffer, StringBuffer outAllStringBuffer) throws Exception{
		
    	//HTTP Connection
	    //String elink_urlParameters = toolEmailLinkname + requestIdListVector.get(i).toString();	    
	    URL url = new URL(elink_url);
	    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    conn.setRequestMethod("POST");
	    conn.setRequestProperty("User-Agent", "Mozilla/5.0");
	    conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");			    
	    conn.setDoOutput(true);        

	    //Send post request
		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		wr.writeBytes(elink_urlParameters);
		wr.flush();
		wr.close();
		
		//Receive the response qnd save/write it to a new xml file
		int responseCode = conn.getResponseCode();
		if (responseCode != 200){
			throw new Exception("!!! ERROR HTTP Response is not successfull. HTTP Response Code : " + responseCode);
		}
	    
		InputStreamReader inStream = new InputStreamReader(conn.getInputStream());
		BufferedReader in = new BufferedReader(inStream);
		String previousLine=""; //used to check whether the XML file received completely
		String inputLine;
				
		while ((inputLine = in.readLine()) != null){
							
			//write the header of the XML only from the first HTTP request 
			if (inputLine.trim().startsWith("<?xml") || inputLine.trim().startsWith("<!DOCTYPE") || inputLine.trim().startsWith("<eLinkResult>")){
				if (firstRequestList){
					//out.write(inputLine); out.newLine();
					outStringBuffer.append(inputLine); outStringBuffer.append(System.lineSeparator());
					if (level == 1){ // write the header of the XML only from the first HTTP request of first cascadeLevel (for the all_citations.xml file)
						//out_all_citations.write(inputLine);	out_all_citations.newLine();
						outAllStringBuffer.append(inputLine); outAllStringBuffer.append(System.lineSeparator());
					}
		        }
	        	previousLine = inputLine.trim();					
		        continue;
			}
			
			//write the footer of XML only from the last HTTP request
			if (inputLine.trim().startsWith("</eLinkResult>")){
				if (lastRequestList){
					//out.write(inputLine); out.newLine();
					outStringBuffer.append(inputLine); outStringBuffer.append(System.lineSeparator());
		        }
	        	previousLine = inputLine.trim();					
		        continue;
			}
			
			//write the content of each HTTP request regularly to the both citations_level_?.xml and all_citations.xml
	        //out.write(inputLine); out.newLine();
			//out_all_citations.write(inputLine);  out_all_citations.newLine();
			outStringBuffer.append(inputLine); outStringBuffer.append(System.lineSeparator());	        
	        outAllStringBuffer.append(inputLine); outAllStringBuffer.append(System.lineSeparator());
	        
	        inputLine = inputLine.trim();
	        
	        // We just comment-out this for getWholePubMedCitations
	       /* if (inputLine.startsWith("<Id>") && previousLine.equals("<Link>")){	        		        	
	        	String id = inputLine.substring(inputLine.indexOf(">")+1, inputLine.lastIndexOf("<"));	        	
		    	if (newPmIdMap.containsKey(id)){
		    		int currentValue = newPmIdMap.get(id).intValue();
		    		newPmIdMap.put(id, new Integer(currentValue+1));	    		
		    	}else{	    		
		    		newPmIdMap.put(id, new Integer(1));
		    	}		        	
	        }	        
	        */
	        if (inputLine.length() > 0)
	        	previousLine = inputLine;	        		        
		}

		//check that the XML file received completely
		if (!previousLine.equals("</eLinkResult>")){
			throw new Exception("!!! ERROR: XML file did not received completely over HTTP request.");
		}
		
        //close them
		//out.flush();  
		//out_all_citations.flush();
		in.close(); 
		inStream.close();
		conn.disconnect();
	    Thread.sleep(1000);  //wait 1 second between HttpRequests		    
		
	}

}


/*
***********This code was used to verify the accuracy of the data retrieval, in a cascading style. You can manually set the cascade-levels. 
        //MANUALLY generate XML file for each level. For the first level use the pdbDataApril2.csv, for the other levels use the xml file of previous level
	    		
		int cascadeLevel = 26;	  
	    pmIdMap = readXmlFile(pathToFile + "citations_level_" + (cascadeLevel-1) + ".xml");	    
	    //read current globalIdMap
	    BufferedReader reader = Files.newBufferedReader(Paths.get(pathToFile + "all_pubmed_id.txt"), ENCODING);
		String line = null; 
	    while ((line = reader.readLine()) != null) {
	    	//skip the header line and any empty lines
	    	if (line.startsWith("*") || line.trim().equals("")) //first line or last line
	    		continue;
	    	int pos = line.indexOf("|"); 
	    	String existingPmId =  line.substring(0, pos-1);
	    	String cnt = line.substring(pos+2);	    	
    		globalPmIdMap.put(existingPmId, new Integer(cnt));
	    }
	    
	    //remove the existing pmIds from pmIdMap
	    for (Iterator<Map.Entry<String, Integer>> iter = pmIdMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, Integer> entry = iter.next();
	    	if (globalPmIdMap.containsKey(entry.getKey()))
	    		iter.remove();
	    }	    	    	    
    	pmIdMap = getPubMedCitations(pathToFile+"citations_level_"+cascadeLevel+".xml", pmIdMap, cascadeLevel);

*/