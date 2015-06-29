package org.biocaddie.citationanalysis.network;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

import org.biocaddie.citationanalysis.retrievedata.CitationAndRefLinkResultXmlParser;
import org.biocaddie.citationanalysis.retrievedata.CitationSummaryResultXmlParser;
import org.biocaddie.citationanalysis.retrievedata.DocSum;
import org.biocaddie.citationanalysis.retrievedata.LinkSet;

/**
 * This class reads two input files: 
 * 1-) all_citations.txt: which contains the links(citations) between the nodes(papers), and
 * 2-) all_citations_summary.txt: which contains additional information about nodes(papers).
 * 
 * Currently, this class constructs two kind of networks:
 * 1-) paper citation network: where nodes are papers and links are citations, and
 * 2-) journal citation network: where nodes are journals and links are citation between the journals. It is constructed by merging the paper citation network into journals.
 * ... later we may construct other kind of networks too...
 * 
 * Optionally you can give startYear endYear, then construct networks using the papers only published between these two years.
 *
 * Generates the network in Pajek.net file format.
 * infoMap wants nodeId's to start from 1 and increase sequentially, because of that we give node id's within the program, 
 * and merge the unique pubmedId or journalId with the nodeName using the double pipe delimiter"||" as shown below.
 * 1 "2186365 || A relational database of transcription factors." 
 * or for journalCitationNetwork
 * 1 "7708900 || American journal of medical genetics"
 * 
 * Example network with three nodes and four directed and weighted links:     
 * *Vertices 3                                                                 
 * 1 "Name of first node"                                               
 * 2 "Name of second node"                                                 
 * 3 "Name of third node"                                                   
 * *Arcs 4                                                                     
 * 1 2 1.0                                                                     
 * 1 3 1.7                                                                     
 * 2 3 2.0                                                                     
 * 3 2 1.2                                                                     
 *
 * NOTE: Citations networks are directed. If "paper A cites paper B" then the direction of the link within the network goes from A to B (A->B).
 * For example: if paper A cited by 900 PubMed papers, then the number of its inLinks=900 (X paper -> A). 
 * Actually, the number of its outLinks should equal to the number of its references, but since not every paper is in PubMed 
 * and we don't retrieve the every paper in PubMed, the number of its outLinks should be equal or less than the number of its references (A -> X paper).  
 */
public class ConstructNetwork {
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	final static Charset ENCODING = StandardCharsets.UTF_8;

	/**
	 * The main function. 
	 * @param args: It accepts two input argument: <all_citations.txt> <all_citations_summary.txt>.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		//Get the inputFileNames from user 
		if(args.length < 2){
    		System.out.println("Call: java org.biocaddie.citation.network.ConstructNetwork <all_citations.txt> <all_citations_summary.txt> optionally <startYear> <endYear>");
    		System.exit(1);
    	}
		int startYear = 0; int endYear = 0; String ext="";
		if (args.length > 2){
			startYear = Integer.valueOf(args[2]).intValue();
			endYear = Integer.valueOf(args[3]).intValue();
			if (startYear > endYear){
	    		System.out.println("startYear cannot be greater than endYear");
	    		System.exit(1);
			}			
			ext = endYear+"_"+startYear+"_";
		}
		
		String fileNameAllCitations = args[0];
		String fileNameAllCitationsSummary = args[1];
		String fileSeparator = System.getProperty("file.separator");		
		String pathToFile = fileNameAllCitations.substring(0, fileNameAllCitations.lastIndexOf(fileSeparator)+1);		

		System.out.println("Start constructing networks...");
		System.out.println("Start Time: " + dateFormat.format(new Date()));
		
		//Step 1: read citations and summary files
		Map<Integer, PaperNode> paperNetworkMap = readCitationAndSummaryFiles(fileNameAllCitations, fileNameAllCitationsSummary, startYear, endYear);

		//Step 2: construct paper citation network
		constructPaperCitationNetwork(paperNetworkMap, pathToFile+ext+"paper_citation_network.net");
	    
		//Step 4: construct journal citation network by merging the paper network into journal network
	 	constructJournalCitationNetwork(paperNetworkMap, pathToFile+ext+"journal_citation_network.net");

		System.out.println("End Time  : " + dateFormat.format(new Date()));	    				
		System.out.println("DONE...");
	}
	
	/**
	 * This method reads these two input files and fills the paperNetworkMap. It also checks the consistency of these two files (all_citations.xml and all_citations_summary.xml).
	 * If a pubmedId exist in all_citations.xml but not in all_citations_summary.xml, 
	 * then remove that pubmed_id and its links from the network, print the notFound pubmedId and print out the number of nodes and links removed from the network. 
	 * @param fileNameAllCitations: xml file which contains the paper citation links.
	 * @param fileNameAllCitationsSummary: xml file which contains the summary info of the papers.
	 * @return : the hashMap Map<String, PaperNode>, where key=pubMedID and value is the PaperNode object.
	 * @throws Exception
	 */
	private static Map<Integer, PaperNode> readCitationAndSummaryFiles(String fileNameAllCitations, String fileNameAllCitationsSummary, int startYear, int endYear) throws Exception {

		System.out.println("Step 1: Reading all_citations.txt and all_citations_summary.txt ..." );

		Map<Integer, PaperNode> paperNetworkMap = new HashMap<Integer, PaperNode>(); //all nodes		
		BufferedReader reader = Files.newBufferedReader(Paths.get(fileNameAllCitationsSummary), ENCODING);
		String line = null; int lineCnt = 0;
	    while ((line = reader.readLine()) != null) {
	    	//skip the header line and any empty lines
	    	if (line.startsWith("*") || line.trim().equals("")) 
	    		continue;

	    	lineCnt++;
        	if ( ( lineCnt % 1000000 ) == 0 )
        		System.out.println("Node count: "+ lineCnt + " " + dateFormat.format(new Date()));

			String[] tokens  = line.split(Pattern.quote("||")); 
			
			int pubYear = Integer.valueOf(tokens[2].trim()).intValue();
			if (startYear > 0 && endYear > 0){
				if (pubYear > endYear || pubYear < startYear)
					continue;
			}
			
			PaperNode node = new PaperNode(tokens[0].trim(), tokens[1].trim(), tokens[5].trim(), tokens[2].trim(), tokens[4].trim(), tokens[3].trim());
			paperNetworkMap.put(Integer.valueOf(tokens[0].trim()), node);
	    }
		
		int numNodesNotIncludedInNetwork = 0;
		int numLinksNotIncludedInNetwork = 0;
		
		//Read all_citations.txt to generate the links
		BufferedReader reader2 = Files.newBufferedReader(Paths.get(fileNameAllCitations), ENCODING);
		line = null;  lineCnt = 0;
	    while ((line = reader2.readLine()) != null) {
	    	//skip the header line and any empty lines
	    	if (line.startsWith("*") || line.trim().equals("")) 
	    		continue;

	    	lineCnt++;
        	if ( ( lineCnt % 1000000 ) == 0 )
        		System.out.println("Link count: " +lineCnt + " " + dateFormat.format(new Date()));
	    	
			String[] tokens  = line.split(Pattern.quote("||")); 
	    	Integer from = Integer.valueOf(tokens[0].trim());	    	
	    	if (!paperNetworkMap.containsKey(Integer.valueOf(from))){
	    		numNodesNotIncludedInNetwork++;
	    		continue;
	    	}

	    	if (tokens[1].trim().length() <= 0) //if citedInList empty
	    		continue;
	    	
	    	String[] citedInList = tokens[1].trim().split(",");	    	
	    	for (int j = 0; j < citedInList.length; j++){	    		
	    		Integer to = Integer.valueOf(citedInList[j].trim());
		    	if (!paperNetworkMap.containsKey(Integer.valueOf(to))){
		    		numLinksNotIncludedInNetwork++;
		    		continue;
		    	}
		    	//if paper A cites paper B then A->B
	    		paperNetworkMap.get(from).inLinks.add(to);	    		
	    		paperNetworkMap.get(to).outLinks.add(from);
	    	}		    		    	
	    }	    
	    
	    if (numNodesNotIncludedInNetwork > 0 || numLinksNotIncludedInNetwork > 0){
	    	System.out.println("Summary of some pubmed ids do not exist, therefore we excluded those nodes and their related links:");
	    	System.out.println("Number of nodes not included in Network: " + numNodesNotIncludedInNetwork);
	    	System.out.println("Number of links not included in Network: " + numLinksNotIncludedInNetwork);
		}
		System.out.println("------------------------------------------------");
		return paperNetworkMap;
	}
	
	/**
	 * This method generates the paper_citation_network in Pajek.net format and write it to a file. 
	 * @param paperNetworkMap: the hashMap that contains the nodes and links of the paper citation network.
	 * @param outFileName: output fileName for paper_citation_network.net in pajek format.
	 * @throws IOException
	 */
	private static void constructPaperCitationNetwork(Map<Integer, PaperNode> paperNetworkMap, String outFileName) throws IOException {
		
		System.out.println("Step 2: Construct paper citation network ..." );
		//Generate pajek.net file for paper_citation_network using paperNetworkMap
		int numNodes = paperNetworkMap.size();
		int numLinks = 0;

	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("*Vertices " + numNodes); out.newLine(); String title = ""; String journalName = "";           
        int id = 1; //we generate id starting from 1, sequentially increasing, because infoMap needs this.
	    for (Iterator<Map.Entry<Integer, PaperNode>> iter = paperNetworkMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	PaperNode pNode = iter.next().getValue();
	    	pNode.id = String.valueOf(id);
	    	//infoMap cannot read nodeNames longer than 500 characters, some paper titles and journal titles are too long!!!
	    	if (pNode.title.length() > 300)
	    		title = pNode.title.substring(0, 299) + "..";
	    	else
	    		title = pNode.title;
	    	
	    	if (pNode.fullJournalName.length() > 100)
	    		journalName = pNode.fullJournalName.substring(0, 99) + "..";
	    	else
	    		journalName = pNode.fullJournalName;
	    	
	    	out.write(pNode.id+" \""+title+" || "+pNode.pubmed_id+" || "+ ((pNode.pubDate.length()>4) ? pNode.pubDate.substring(0, 4) : pNode.pubDate) +" || "+journalName+" || "+pNode.nlmUniqueID + "\""); out.newLine();
	    	numLinks = numLinks + pNode.outLinks.size();	    
	    	id++;
	    }
	    
	    int singleNodes = 0;
	    out.write("*Arcs " + numLinks); out.newLine();	
	    String weight = "1.0"; //default weight
	    for (Iterator<Map.Entry<Integer, PaperNode>> iter = paperNetworkMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<Integer, PaperNode> entry = iter.next();
	    	String from = entry.getValue().id;
	    	
	    	if (entry.getValue().inLinks.size() == 0 && entry.getValue().outLinks.size() == 0)
	    		singleNodes++;
	    	
	    	for (int i = 0; i < entry.getValue().outLinks.size(); i++) {
	    		String to = paperNetworkMap.get(entry.getValue().outLinks.get(i)).id;
		    	out.write(from + " " + to + " " + weight); out.newLine();	    		
	    	}	    	
	    }
	    
        out.flush();       
        out.close();    
		
        System.out.println("Number of nodes in the paper network: " + numNodes);
        System.out.println("Number of links in the paper network: " + numLinks);
		System.out.println("Number of nodes with no incoming/outgoing links: " + singleNodes);
		System.out.println("------------------------------------------------");					    
	}
	
	/**
	 * This method converts paperNetworkMap to journalNetworkMap by merging all paper nodes from the same journal into a single journalNode,
	 * then generates the journal_citation_network in Pajek.net format and write it to a file.
	 * @param paperNetworkMap: the hashMap that contains the nodes and links of the paper citation network.
	 * @param outFileName: output fileName for journal_citation_network.net in pajek format.
	 * @throws IOException
	 */
	private static void constructJournalCitationNetwork(Map<Integer, PaperNode> paperNetworkMap, String outFileName) throws IOException {
		
		//!! Journal Id, nlmUniqueID, can be alphanumeric, such as (2984771R: Military medicine), so we should use String, but, for infoMap ??? we need to think!!!
		
		System.out.println("Step 3: Construct journal citation network ..." );		
		
		//1-)First, convert paperNetworkMap to journalNetworkMap (by merging all paper nodes from the same journal into a single journalNode, and also merge their links)
		Map<String, JournalNode> journalNetworkMap = new HashMap<String, JournalNode>(); //nodes are journals
	    for (Iterator<Map.Entry<Integer, PaperNode>> iter = paperNetworkMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	
	    	Map.Entry<Integer, PaperNode> entry = iter.next();
	    	
	    	//if the fromJournal does not exist in the journalMap, first put it
	    	String fromJournalId = entry.getValue().nlmUniqueID;
	    	String fromJournalName = entry.getValue().fullJournalName;	    	
	    	if (!journalNetworkMap.containsKey(fromJournalId))
	    		journalNetworkMap.put(fromJournalId, new JournalNode(fromJournalId, fromJournalName));	    		
	        
	    	//generate outLinks
	    	for (int i = 0; i < entry.getValue().outLinks.size(); i++) {
	    		Integer toPaperId = entry.getValue().outLinks.get(i);
	    		
		    	//if the toJournal does not exist in the journalMap, first put it
	    		String toJournalId = paperNetworkMap.get(toPaperId).nlmUniqueID;
	    		String toJournalName = paperNetworkMap.get(toPaperId).fullJournalName;
		    	if (!journalNetworkMap.containsKey(toJournalId))
		    		journalNetworkMap.put(toJournalId, new JournalNode(toJournalId, toJournalName));	    		

		    	//then for each paper citation link, add the weights between journals cumulatively.
		    	if (journalNetworkMap.get(fromJournalId).outLinks.containsKey(toJournalId)){
		    		int currentWeight = journalNetworkMap.get(fromJournalId).outLinks.get(toJournalId).intValue();
		    		journalNetworkMap.get(fromJournalId).outLinks.put(toJournalId, currentWeight + 1); 
		    	}else{
		    		journalNetworkMap.get(fromJournalId).outLinks.put(toJournalId, 1); // if this is the first outLink between journals
		    	}
	    	}
	    	
	    	//generate inLinks
	    	for (int i = 0; i < entry.getValue().inLinks.size(); i++) {
	    		Integer toPaperId = entry.getValue().inLinks.get(i);
	    		
		    	//if the toJournal does not exist in the journalMap, first put it
	    		String toJournalId = paperNetworkMap.get(toPaperId).nlmUniqueID;
	    		String toJournalName = paperNetworkMap.get(toPaperId).fullJournalName;
		    	if (!journalNetworkMap.containsKey(toJournalId))
		    		journalNetworkMap.put(toJournalId, new JournalNode(toJournalId, toJournalName));	    		

		    	//then for each paper citation link, add the weights between journals cumulatively.
		    	if (journalNetworkMap.get(fromJournalId).inLinks.containsKey(toJournalId)){
		    		int currentWeight = journalNetworkMap.get(fromJournalId).inLinks.get(toJournalId).intValue();
		    		journalNetworkMap.get(fromJournalId).inLinks.put(toJournalId, currentWeight + 1); 
		    	}else{
		    		journalNetworkMap.get(fromJournalId).inLinks.put(toJournalId, 1); // if this is the first inLink between journals
		    	}
	    	}
	    	
	    }
	    
	    //2-)Then, generate pajek.net file for journal_citation_network using journalNetworkMap
		int numNodes = journalNetworkMap.size();
		int numLinks = 0;

	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("*Vertices " + numNodes); out.newLine();       String journalName = "";
        int id = 1; //we generate id starting from 1, sequentially increasing, because infoMap needs this.
	    for (Iterator<Map.Entry<String, JournalNode>> iter = journalNetworkMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	JournalNode jNode = iter.next().getValue();
	    	jNode.id = String.valueOf(id);
	    	
	    	//infoMap cannot read nodeNames longer than 500 characters, some journal names are too long!!!
	    	if (jNode.fullJournalName.length() > 400)
	    		journalName = jNode.fullJournalName.substring(0, 399) + "..";
	    	else
	    		journalName = jNode.fullJournalName;
	    	
	    	out.write(jNode.id + " \"" + journalName + " || " +jNode.nlmUniqueID + "\""); out.newLine();
	    	numLinks = numLinks + jNode.outLinks.size();
	    	id++;
	    }
	    
	    int singleNodes = 0;
	    out.write("*Arcs " + numLinks); out.newLine();	
	    for (Iterator<Map.Entry<String, JournalNode>> iter = journalNetworkMap.entrySet().iterator(); iter.hasNext(); ) {	    		    	
	    	Map.Entry<String, JournalNode> entry = iter.next();
	    	String from = entry.getValue().id;
	    	
	    	if (entry.getValue().inLinks.size() == 0 && entry.getValue().outLinks.size() == 0)
	    		singleNodes++;
	    	
		    for (Iterator<Map.Entry<String, Integer>> iter2 = entry.getValue().outLinks.entrySet().iterator(); iter2.hasNext(); ) {	    		    	
		    	Map.Entry<String, Integer> entry2 = iter2.next();
		    	String to = journalNetworkMap.get(entry2.getKey()).id;
		    	String weight = entry2.getValue().toString();
		    	out.write(from + " " + to + " " + weight); out.newLine();	    		
	    	}	    	
	    }
	    
        out.flush();       
        out.close();    

        System.out.println("Number of nodes in the journal network: " + numNodes);
        System.out.println("Number of links in the journal network: " + numLinks);
		System.out.println("Number of nodes with no incoming/outgoing links: " + singleNodes);
		System.out.println("------------------------------------------------");					    
	}

}

/**
 * Java class to hold the node attributes in the paper_citation_network.
 */
class PaperNode {	
	String id; // this is a unique id starting from 1, and increase sequentially.We generate this id, because infoMap needs it.
	String pubmed_id; 
	String title;
	String lastAuthor;
	String pubDate;		
	String nlmUniqueID; // unique journal id, but keep in mind that it is alphanumeric!!!
	String fullJournalName;
	List<Integer> inLinks = new ArrayList<Integer>();
	List<Integer> outLinks = new ArrayList<Integer>();
   
	PaperNode(){ }
	PaperNode(String p_pubmed_id, String p_title, String p_lastAuthor, String p_pubDate, String p_nlmUniqueID, String p_fullJournalName){
		pubmed_id = p_pubmed_id;		
		title = p_title;
		lastAuthor = p_lastAuthor;
		pubDate = p_pubDate;
		nlmUniqueID = p_nlmUniqueID;
		fullJournalName = p_fullJournalName;
    }
}

/**
 * Java class to hold the node attributes in the journal_citation_network.
 */
class JournalNode {	
	String id; // this is a unique id starting from 1, and increase sequentially.We generate this id, because infoMap needs it.
	String nlmUniqueID; //journal id (nlmUniqueID). Keep in mind that it is alphanumeric!!! nlmUniqueID
	String fullJournalName;
	Map<String, Integer> inLinks = new HashMap<String, Integer>();  // neighborJournalId - weight
	Map<String, Integer> outLinks = new HashMap<String, Integer>(); // neighborJournalId - weight
	   
	JournalNode(){ }
	JournalNode(String p_nlmUniqueID, String p_fullJournalName){
		nlmUniqueID = p_nlmUniqueID;
		fullJournalName = p_fullJournalName;
    }
}
