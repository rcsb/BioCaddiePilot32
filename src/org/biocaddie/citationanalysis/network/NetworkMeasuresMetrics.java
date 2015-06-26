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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * This class reads a network in Pajek format, and generates the following metrics for each node and write in an output file:
 * - Degree centrality: the number of incoming links which equals to the citation count.
 * - PageRank: pageRank computed by power method as explained below. implicitly take the effect of citation cascades.
 * - PageRankNormalized: pageRankof each node is a small probability and the sum of all node's pageRank is 1. We just multiply the pageRank of each node with the number of all links in the network.
 * - Betweenness centrality: the number of shortest paths that goes through a node, may indicate the papers between disciplines.
 * - TODO: may be other node metrics...
 * - TODO: also we may compute global network measures/metrics such as:
 * 		  - the average degree (number of links) per node 
 * 		  - ....	
 * 
 * Example output:
 * * NodeId | InDegree(CiteCnt) | pageRank             | pageRankNormalized
 * 22530020 |     3             | 9.502959327539258E-8 | 1.939725622196218
 * 22530021 |     1             | 9.629902990542835E-8 | 1.9656371164178017
 *  
 */
public class NetworkMeasuresMetrics {
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    /**
     * The main method, start point.
     * @param args
     * @throws IOException
     */
	public static void main(String[] args) throws IOException {
		
		//Get the inputFileNames from user 
		if(args.length < 1){
    		System.out.println("Call: java org.biocaddie.citation.network.NetworkMeasuresMetrics <network.net> <optional pageRankDampingFactor>");
    		System.exit(1);
    	}
		String inputFileName = args[0];		
		String fileSeparator = System.getProperty("file.separator");		
		String pathToFile = inputFileName.substring(0, inputFileName.lastIndexOf(fileSeparator)+1);		
		
		//String outFileName = inputFileName.substring(inputFileName.lastIndexOf(fileSeparator)+1, inputFileName.lastIndexOf(".")) + "_clean.net";
		String outFileName = inputFileName.substring(inputFileName.lastIndexOf(fileSeparator)+1, inputFileName.lastIndexOf(".")) + "_cocitation_short.net";
		
		System.out.println("Start computing network measures/metrics...");
		Network network = new Network(inputFileName);		
		System.out.println("Start Time: " + dateFormat.format(new Date()));
		
		
		//Step 1: Read the network in Pajek.net file format
		readPajekNetFile(network); 
		//generateCleanJournalNetwork(network, pathToFile+outFileName);
		Network cocitationNetwork = convertCitationToCocitation(network, pathToFile+outFileName);
		network = cocitationNetwork;
		
		
		
	/*    BufferedWriter out = new BufferedWriter(new FileWriter(new File("/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/paper_citation_network_light_noDangling.net")));

		
	    BufferedReader reader = Files.newBufferedReader(Paths.get("/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/paper_citation_network.net"), ENCODING);
		String line = null; String currentPart = ""; int lineCnt = 0;
	    while ((line = reader.readLine()) != null) {	    	
	    	line = line.trim();
	    	lineCnt++;
        	if ( ( lineCnt % 1000000 ) == 0 )
        		System.out.println( lineCnt + " " + dateFormat.format(new Date()));
	    	
	    	if (line.length() > 9 && line.substring(0, 9).equalsIgnoreCase("*Vertices")){
	    		currentPart = "nodes";	                   	            
	    		System.out.println("Reading nodes...");
	    		out.write(line); out.newLine();
	    		continue;
	    	} 

	    	if (line.length() > 6 && ( line.substring(0, 6).equalsIgnoreCase("*Edges") || line.substring(0, 5).equalsIgnoreCase("*Arcs"))){
	    		currentPart = "links";
	    		System.out.println("Reading links...");
	    		out.write(line); out.newLine();
	    		continue;	    		
	    	} 
	    	
	    	if (currentPart.equals("nodes")){
	    	    int nameStart = line.indexOf("\"");
	    	    int nameEnd = line.lastIndexOf("\"");
	    	    
	    	    Integer id = Integer.valueOf(line.substring(0, nameStart-1));	    	    
	    	    String pubmed_id = line.substring(nameStart+1, nameEnd).split(Pattern.quote("||"))[1].trim();
	    	    
	    	    if (network.nodeMap.get(id).inLinks.size() <= 0 && network.nodeMap.get(id).outLinks.size() <= 0){
	    	    	out.write(id + " \"" + pubmed_id + "\""); out.newLine();
	    	    }
	    	}else{
    	    	out.write(line); out.newLine();
	    	}
	    	
	    }
		
	    reader.close();
        out.flush();       
        out.close();   
        */
		//Start Strongly Connected Components
   /* 	Queue<Node> queue = new LinkedList<Node>();	    	
    	int numOfVisitedNodes = 0;
    	Node startNode = network.nodeMap.get(new Integer(16));    	
    	queue.add(startNode);  
    	startNode.visited = true; 
    	numOfVisitedNodes++;
    	
    	while (!queue.isEmpty()) {
    		Node v = queue.remove();

    		//like undirected, use both inLinks and outLinks
		    for (Iterator<Map.Entry<Integer, Double>> iter = v.outLinks.entrySet().iterator(); iter.hasNext(); ){			    	
		    	Node w = network.nodeMap.get(iter.next().getKey());
		    	if (!w.visited){
		    		queue.add(w); 
		    		w.visited = true;
		    		numOfVisitedNodes++;
		    	}
		    }
		    for (Iterator<Map.Entry<Integer, Double>> iter = v.inLinks.entrySet().iterator(); iter.hasNext(); ){
		    	Node w = network.nodeMap.get(iter.next().getKey());
		    	if (!w.visited){
		    		queue.add(w); 
		    		w.visited = true;
		    		numOfVisitedNodes++;
		    	}
		    }
    	}
    	
    	System.out.println("size: " + network.nodeMap.size());
    	System.out.println("numOfVisitedNodes: " + numOfVisitedNodes);
    	//End Strongly Connected Components
    	
		//Map<Integer, Integer> indegreeMap = new HashMap<Integer, Integer>(); 
	//    BufferedWriter out = new BufferedWriter(new FileWriter(new File("/Users/ali/Documents/BioCaddie/data/citation/april_29/cites_refs/paper_citation_network_light_noDanglingSelflinkLoop.net")));
	//	out.write("*Vertices "); out.newLine();
*/
		int noInlinks = 0;
		int noOutLinks = 0;
		int noInOutLinks = 0;
		int inOutLinks = 0; int selfLink = 0; int loopLink = 0; int totIn=0; int totOut=0; int selflinkWeight = 0; int totInLinkSize = 0;
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		Node node = iter.next().getValue();
    		//node.pageRank = -1;
    		
    		if (node.inLinks.size() <= 0)
    			noInlinks++;    			
    		if (node.outLinks.size() <= 0)
    			noOutLinks++;    		
    		if (node.inLinks.size() <= 0 && node.outLinks.size() <= 0)
    			noInOutLinks++;    	
    		if (node.inLinks.size() > 0 && node.outLinks.size() > 0)
    			inOutLinks++;    		
    		    		
    		//self-citation A->A
    		if (node.inLinks.containsKey(node.id) ){
    			selfLink++;
    		//	node.inLinks.remove(node.id);
    		}
    	//	if (node.outLinks.containsKey(node.id) )
    	//	    node.outLinks.remove(node.id);			    
    			
    		//citation loop (A->B and B->A)
    		for (Iterator<Map.Entry<Integer, Double>> iterInLinks = node.inLinks.entrySet().iterator(); iterInLinks.hasNext(); ) {
    			Entry<Integer, Double> entry = iterInLinks.next(); 
    			Integer other_id = entry.getKey();
    			Double weight = entry.getValue();
    			totInLinkSize += weight;
    			if (network.nodeMap.get(other_id).inLinks.containsKey(node.id)){
    				loopLink++;
    	//			network.nodeMap.get(other_id).inLinks.remove(node.id);
    	//			node.outLinks.remove(other_id);
    			}
    			if (node.id == other_id)
    				selflinkWeight += weight;
    		}
    		
  //  		node.pageRank = newId;
 //   		out.write(node.pageRank + " \"" + node.name+ "\""); out.newLine();
   // 		newId++;
    		totIn += node.inLinks.size();
    		totOut += node.outLinks.size();
    	}

	/*	out.write("*Edges "); out.newLine();
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		Node node = iter.next().getValue();
    		for (Iterator<Map.Entry<Integer, Double>> iterOutLinks = node.outLinks.entrySet().iterator(); iterOutLinks.hasNext(); ) {
    			Map.Entry<Integer, Double> e = iterOutLinks.next();
    			out.write(node.pageRank + " " + network.nodeMap.get(e.getKey()).pageRank+ " 1.0"); out.newLine();
    		}		
		}	
        out.flush();       
        out.close();   */
		System.out.println("noInlinks: " + noInlinks);
		System.out.println("noOutLinks: " + noOutLinks);
		System.out.println("noInOutLinks: " + noInOutLinks);
		System.out.println("inOutLinks: " + inOutLinks);
		System.out.println("selfLink: " + selfLink);
		System.out.println("selflinkWeight: " + selflinkWeight);
		System.out.println("loopLink: " + loopLink);
		System.out.println("totOut: " + totOut);
		System.out.println("totIn: " + totIn);
		System.out.println("totOut: " + totOut);
		System.out.println("totInLinkSize: " + totInLinkSize);
		System.out.println("netSize: " + network.nodeMap.size());



		
		//*************** START					
	/*	Map<Integer, Integer> pubmed_id_map = new HashMap<Integer, Integer>(); //unique pubMed Id's (key: pubMedId value: count)		
    	BufferedReader reader3 = Files.newBufferedReader(Paths.get("/Users/ali/Documents/BioCaddie/data/citation/april_29/refs/PdbId_PubMedId_April29.csv"), ENCODING);
    	String line = null;
		while ((line = reader3.readLine()) != null) {			
			//skip the header line and any empty lines
			if (line.trim().startsWith("PDB") || line.trim().equals(""))
				continue;
			
			String[] tokens  = line.split(","); 
		   // String pdb_id    = tokens[0].replaceAll("\"", "").trim();		    
		    String s_pubmed_id =  tokens[1].replaceAll("\"", "").trim();
		    
		    if (s_pubmed_id.length() <= 0) // if the primary citation PubMed id is empty, continue
		    	continue;
		    
		    Integer pubmed_id = Integer.valueOf(s_pubmed_id);
	    	if (pubmed_id_map.containsKey(pubmed_id)){
		   		Integer currentValue = pubmed_id_map.get(pubmed_id);
		   		pubmed_id_map.put(pubmed_id, currentValue+1);	    		
		   	}else{	    		
		   		pubmed_id_map.put(pubmed_id, 1);
		   	}	
		}	
		
		Map<Integer, Integer> pubmed_id_mapNew = new HashMap<Integer, Integer>();
    	for (Iterator<Map.Entry<Integer, Node>> iterInLinks = network.nodeMap.entrySet().iterator(); iterInLinks.hasNext(); ) {	
    		Map.Entry<Integer, Node> entryInLinks = iterInLinks.next();
    		String[] tokens = entryInLinks.getValue().name.split(Pattern.quote("||"));
    		Integer id = entryInLinks.getKey();
    		Integer pudMedId = Integer.valueOf(tokens[1].trim());
    		
	    	if (pubmed_id_map.containsKey(pudMedId))
	    		pubmed_id_mapNew.put(id, pubmed_id_map.get(pudMedId));	    		
    	}		
		
    	int cascadeLevel = 1;	    
	    while (pubmed_id_mapNew.size() > 0){
	    	Map<Integer, Integer> newCascadeList = new HashMap<Integer, Integer>();
	    	int nodes = 0; int links = 0;
	    	//System.out.println ("Cascade Level "+cascadeLevel+" | " + pubmed_id_mapNew.size() + " | " + links);

	    	for (Iterator<Map.Entry<Integer, Integer>> iter = pubmed_id_mapNew.entrySet().iterator(); iter.hasNext(); ) {	
	    		Map.Entry<Integer, Integer> entry = iter.next();
	    		links += entry.getValue();
	    		
		    	for (Iterator<Map.Entry<Integer, Double>> iterInLinks = network.nodeMap.get(entry.getKey()).outLinks.entrySet().iterator(); iterInLinks.hasNext(); ) {	
		    		Map.Entry<Integer, Double> entryInLinks = iterInLinks.next();
		    		if (newCascadeList.containsKey(entryInLinks.getKey())){
		    			Integer currentValue = newCascadeList.get(entryInLinks.getKey());
		    			newCascadeList.put(entryInLinks.getKey(), currentValue+1);
		    		}else{
		    			newCascadeList.put(entryInLinks.getKey(), 1);
		    		}
		    	}
		    			
		    }
	    		
	    	System.out.println ("Cascade Level "+cascadeLevel+" | " + pubmed_id_mapNew.size() + " | " + links);
	    	pubmed_id_mapNew = newCascadeList;
	    	cascadeLevel++;
	    }
		*/
	    //*************END
	    
	/*	Map<String, YearCitations> years_map = new HashMap<String, YearCitations>(); // (key: nodeId value: count)
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
			Node n = iter.next().getValue();
			String pubYear = n.name;
			Double citeCount = Double.valueOf(n.inLinks.size());
		    if (years_map.containsKey(n.name)){  	
		    	YearCitations yearCitations = years_map.get(pubYear);
		    	yearCitations.totalCitationCount += citeCount ;
	    		yearCitations.numOfPapers++;
		    	if (n.inLinks.size()==0 && n.outLinks.size()==0)
		    		yearCitations.numOfDanglingPapers++;		    	
		    }else{
		    	if (n.inLinks.size()==0 && n.outLinks.size()==0)
		    		years_map.put(pubYear, new YearCitations(pubYear, citeCount, 1.0, 1.0));
		    	else
		    		years_map.put(pubYear, new YearCitations(pubYear, citeCount, 1.0, 0.0));		    				    	
		    }		    
		}
		
	    Map<String, YearCitations> sorted_years_map = new TreeMap<String, YearCitations>(years_map);
	    years_map = sorted_years_map;
	    double numOfYears = years_map.size();

	    for (Iterator<Map.Entry<String, YearCitations>> iter = years_map.entrySet().iterator(); iter.hasNext(); ) {	    		
	    	YearCitations yearCitations = iter.next().getValue();

	    	//System.out.println(yearCitations.year + "|" + yearCitations.totalCitationCount + "|"+ yearCitations.numOfPapers + "|"+ yearCitations.totalPageRank);
	    	System.out.println(yearCitations.year +"|" +yearCitations.numOfPapers.intValue() + "|" + yearCitations.totalCitationCount + "|" + yearCitations.numOfDanglingPapers);
	    }
	    */
		//Step 2: Compute PageRank of each node within the network
		//double alpha = 0.7; // teleportation probability, default value
		//if (args.length > 1)
		//	alpha = Double.valueOf(args[1]);
		//pageRank(network, alpha);
		
		//Step 3: Compute the Betweenness centrality of each node within the network
		//betweennessCentrality(network);
		
		//Step 4: Write the measures/metrics of each node to a file // Node Id | CentralityDegree | PageRank | BetweennessCentrality		
		//writeResultsToFile(network, pathToFile+outFileName);
		
		System.out.println("End Time  : " + dateFormat.format(new Date()));	    		
		System.out.println("Done!..");
	}

	//This method converts the citation network to co-citation network by taking the combinations of each paper's outLinks.
	//Co-citation network will be undirected and weighted.
	public static Network convertCitationToCocitation(Network network, String newFileName) throws IOException {
		//create new network for co-citation, since it is undirected use only outLinks, inLinks will be always empty
		Network coCitationNetwork = new Network(newFileName);		
				
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		Node node = iter.next().getValue();
    		if(node.outLinks.size() < 2 ) //then there is no combination, in order to find co-citation, outLink >=2 
    			continue;

    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
    			Integer otherId = iter2.next().getKey();
    			Node otherNode = network.nodeMap.get(otherId);
    			if (!coCitationNetwork.nodeMap.containsKey(otherId)){
    				Node newNode = new Node(otherNode.id, otherNode.name, otherNode.nodeWeight); 
    				coCitationNetwork.nodeMap.put(otherId, newNode);    			
    			}    			
    		}     			

    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
    			Node cociteNode1 = coCitationNetwork.nodeMap.get(iter2.next().getKey());
    			for (Iterator<Map.Entry<Integer, Double>> iter3 = node.outLinks.entrySet().iterator(); iter3.hasNext(); ){
        			Node cociteNode2 = coCitationNetwork.nodeMap.get(iter3.next().getKey());
        			if (cociteNode1.id.intValue() == cociteNode2.id.intValue())
        				continue;
        			if (cociteNode1.outLinks.containsKey(cociteNode2.id))
        				cociteNode1.outLinks.put(cociteNode2.id, cociteNode1.outLinks.get(cociteNode2.id) + 1.0);
        			else
        				cociteNode1.outLinks.put(cociteNode2.id, 1.0);        			
    			}
    		}    			    		
		}		
		
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(newFileName)));
		out.write("*Vertices " + coCitationNetwork.nodeMap.size()); out.newLine();
        int id = 1; //we generate id starting from 1, sequentially increasing, because infoMap needs this.
        int numLinks = 0; int tooLong = 0;
		for (Iterator<Map.Entry<Integer, Node>> iter = coCitationNetwork.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	Node jNode = iter.next().getValue();
	    	jNode.citeCountRank = id;
	    	
	    	//if the nodeName.length > 500, infoMap gives error, because of that we are shortening the paper names and journal names.
    		String[] tokens = jNode.name.split(Pattern.quote("||"));    		
    		tokens[0] = tokens[0].replaceAll("[^a-zA-Z0-9]+"," "); //remove all special characters.
    		tokens[3] = tokens[3].replaceAll("[^a-zA-Z0-9]+"," ");
    		if (tokens[0].length() > 300)
    			tokens[0] = tokens[0].substring(0, 299);
    		if (tokens[3].length() > 100)
    			tokens[3] = tokens[3].substring(0, 99);    		
    		jNode.name = tokens[0].trim()+" || "+tokens[1].trim()+" || "+tokens[2].trim()+" || "+tokens[3].trim()+" || "+tokens[4].trim();
    		if (jNode.name.length() > 490)
    			System.out.print("!!! STILL LONG..");

	    	//out.write(id + " \"" + jNode.name+"\""); out.newLine();
    		out.write(id + " \"" + tokens[1].trim()+"\""); out.newLine();
	    	numLinks = numLinks + jNode.outLinks.size();
	    	id++;
	    }
		
	    out.write("*Edges " + numLinks); out.newLine();	
		for (Iterator<Map.Entry<Integer, Node>> iter = coCitationNetwork.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	Node jNode = iter.next().getValue();
	    	Integer from = jNode.citeCountRank;
	    		    	
		    for (Iterator<Map.Entry<Integer, Double>> iter2 = jNode.outLinks.entrySet().iterator(); iter2.hasNext(); ) {	    		    	
		    	Map.Entry<Integer, Double> entry2 = iter2.next();
		    	Integer to = coCitationNetwork.nodeMap.get(entry2.getKey()).citeCountRank;
		    	String weight = entry2.getValue().toString();
		    	out.write(from + " " + to + " " + weight); out.newLine();	    		
	    	}	    	
	    }		
        out.flush();       
        out.close();    		
		
        System.out.println("tooLong Lines:" + tooLong);
		return coCitationNetwork;
	}
	
	/**
	 * Since we are interested in information flow between journals,
	 * 1-) we exclude self-citations
	 * 2-) we exclude journals which receive citations < 12
	 * 3-) we exclude journals which make citations < 12
	 * 4-) we exclude four interdisciplinary journals: Nature, Science, PNAS and Plos One
	 */
	public static void generateCleanJournalNetwork(Network network, String newFileName) throws IOException {		
		
		//exclude four major interdisciplinary journals: Nature(0410462), Science(0404511), PNAS(7505876)  and PlosOne (101285081)
	/*	List<Integer> numbers = new ArrayList<>();
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		Node node = iter.next().getValue();
    		if (node.name.contains("|| 0410462") || node.name.contains("|| 0404511") || node.name.contains("|| 7505876") || node.name.contains("|| 101285081"))
    			numbers.add(node.id);
		}
		for (Integer i : numbers) {
		    System.out.println(i);
			Node journal = network.nodeMap.get(i);
			for (Iterator<Map.Entry<Integer, Double>> iter = journal.outLinks.entrySet().iterator(); iter.hasNext(); ) 
				network.nodeMap.get(iter.next().getKey()).inLinks.remove(journal.id);
			for (Iterator<Map.Entry<Integer, Double>> iter = journal.inLinks.entrySet().iterator(); iter.hasNext(); ) 
				network.nodeMap.get(iter.next().getKey()).outLinks.remove(journal.id);
			network.nodeMap.remove(i);		    
		}
*/				
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		Node node = iter.next().getValue();
    		  		
    		//remove self-links
    		if (node.inLinks.get(node.id) != null)
    			node.inLinks.remove(node.id);    		
    		if (node.outLinks.get(node.id) != null)
    			node.outLinks.remove(node.id);

    		//exclude journals which receive or make citations <= 12
    		int outWeight = 0; int inWeight = 0;    		
    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.inLinks.entrySet().iterator(); iter2.hasNext(); ){
    			Map.Entry<Integer, Double> e = iter2.next();
    			if (network.nodeMap.get(e.getKey()).inLinks.containsKey(node.id)){   				
    				network.nodeMap.get(e.getKey()).inLinks.remove(node.id);
    				node.outLinks.remove(e.getKey());
    			}
    			inWeight += e.getValue();        		
    		}
    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ) 
    			outWeight += iter2.next().getValue();
		
   		    		
    		if (outWeight < 1 && inWeight < 1){
        		for (Iterator<Map.Entry<Integer, Double>> iterOutLinks = node.outLinks.entrySet().iterator(); iterOutLinks.hasNext(); ) 
        			network.nodeMap.get(iterOutLinks.next().getKey()).inLinks.remove(node.id);
        		for (Iterator<Map.Entry<Integer, Double>> iterInLinks = node.inLinks.entrySet().iterator(); iterInLinks.hasNext(); ) 
        			network.nodeMap.get(iterInLinks.next().getKey()).outLinks.remove(node.id);
    			iter.remove();
    		}
		}
		
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(newFileName)));
		out.write("*Vertices " + network.nodeMap.size()); out.newLine();
        int id = 1; //we generate id starting from 1, sequentially increasing, because infoMap needs this.
        int numLinks = 0;
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	Node jNode = iter.next().getValue();
	    	jNode.citeCountRank = id;
	    	out.write(id + " \"" + jNode.name+"\""); out.newLine();
	    	numLinks = numLinks + jNode.inLinks.size();
	    	id++;
	    }
		
	    out.write("*Arcs " + numLinks); out.newLine();	
		for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	Node jNode = iter.next().getValue();
	    	Integer from = jNode.citeCountRank;
	    		    	
		    for (Iterator<Map.Entry<Integer, Double>> iter2 = jNode.outLinks.entrySet().iterator(); iter2.hasNext(); ) {	    		    	
		    	Map.Entry<Integer, Double> entry2 = iter2.next();
		    	Integer to = network.nodeMap.get(entry2.getKey()).citeCountRank;
		    	String weight = entry2.getValue().toString();
		    	out.write(from + " " + to + " " + weight); out.newLine();	    		
	    	}	    	
	    }		
        out.flush();       
        out.close();    		
	}
	
	/**
	 * Following the implementation of InfoMap written in C++ (by Martin Rosvall):
	 * This method reads network in Pajek format                                                
	 * each directed link occurring only once, and link weights > 0.               
	 * For more information, see http://vlado.fmf.uni-lj.si/pub/networks/pajek/.   
	 * Node weights are optional and sets the relative proportion to which         
	 * each node receives teleporting random walkers. Default value is 1.          
	 * Example network with three nodes and four directed and weighted links:      
	 * *Vertices 3                                                                 
	 * 1 "Name of first node" 1.0                                                  
	 * 2 "Name of second node" 2.0                                                 
	 * 3 "Name of third node" 1.0                                                  
	 * *Arcs 4                                                                     
	 * 1 2 1.0                                                                     
	 * 1 3 1.7                                                                     
	 * 2 3 2.0                                                                     
	 * 3 2 1.2
	 *                                                                      
	 * @param network: network object which contains the inputFileName
	 * @throws IOException
	 */
	public static void readPajekNetFile(Network network) throws IOException {
		
	    BufferedReader reader = Files.newBufferedReader(Paths.get(network.name), ENCODING);
		String line = null; String currentPart = ""; int linkCount = 0; int lineCnt = 0;
	    while ((line = reader.readLine()) != null) {	    	
	    	line = line.trim();
	    	if (line.equals("")) //skip the empty lines, if there is any
	    		continue;
	    	
	    	lineCnt++;
        	if ( ( lineCnt % 1000000 ) == 0 )
        		System.out.println( lineCnt + " " + dateFormat.format(new Date()));
	    	
	    	if (line.length() > 9 && line.substring(0, 9).equalsIgnoreCase("*Vertices")){
	    		network.numNode = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
	    		currentPart = "nodes";
	    		System.out.println("Reading nodes...");
	    		continue;
	    	} 

	    	if (line.length() > 6 && ( line.substring(0, 6).equalsIgnoreCase("*Edges") || line.substring(0, 5).equalsIgnoreCase("*Arcs"))){
	    		network.numLink = new Integer(line.substring(line.indexOf(" ") + 1)).intValue();
	    		currentPart = "links";
	    		System.out.println("Reading links...");
	    		continue;
	    	} 
	    	
	    	//Reading nodes
	    	if (currentPart.equals("nodes")){
	    	    int nameStart = line.indexOf("\"");
	    	    int nameEnd = line.lastIndexOf("\"");
	    	    
	    	    Integer id = Integer.valueOf(line.substring(0, nameStart-1));	    	    
	    	    String name = line.substring(nameStart+1, nameEnd);
	    	    //String name = line.substring(nameStart+1, nameEnd).split(Pattern.quote("||"))[2].trim();
	    	    
	    	    String nodeWeight_str = line.substring(nameEnd+1).trim();
	    	    Double nodeWeight = 1.0; //default value
	    	    if (nodeWeight_str.length() > 0 && Double.valueOf(nodeWeight_str) > 0.0)
	    	    	nodeWeight = Double.valueOf(nodeWeight_str);	    	    
	    	    	
	    	    Node node = new Node(id, name, nodeWeight);
	    	    network.nodeMap.put(id, node);
	    	    network.totalNodeWeight += nodeWeight;
	    	}
	    	
	    	//Reading links
	    	if (currentPart.equals("links")){	    	

	    		String[] tokens = line.split(" ");
	    		Integer from = Integer.valueOf(tokens[0].trim());
	    		Integer to = Integer.valueOf(tokens[1].trim());
	    		if (!network.nodeMap.containsKey(from) || !network.nodeMap.containsKey(to))
	    			continue;
	    		
	    		Double weight = 1.0; //default value 	    				    				
	    		if (tokens.length > 2 && tokens[2].trim().length() > 0 && Double.valueOf(tokens[2].trim()) > 0.0)
	    			weight = Double.valueOf(tokens[2].trim());
	    		
	    		//add link to both ends
	    		network.nodeMap.get(from).outLinks.put(to, weight);
	    		network.nodeMap.get(to).inLinks.put(from, weight);
	    	
	    		linkCount++;
	    	}
		
	    }
	    
	    if (network.numNode != network.nodeMap.size()){
	    	System.out.println("Number of nodes not matching, exiting!!!");
	    	System.exit(1);
	    }
	    if (network.numLink != linkCount){
	    	System.out.println("Number of links not matching, exiting!!!");
	    	System.exit(1);
	    }
   	    		
	}	
	
	/**
	 * Following the Greedy::eigenvector(void) implementation of InfoMap written in C++ by Martin Rosvall. http://www.tp.umu.se/~rosvall/code.html
	 * This method computes the pageRank of each node within the network using the Power method.
	 * Power method iterates while((Niterations < 200) && (sqdiff > 1.0e-15 || Niterations < 50))
	 * @param network: The network object which contains the nodes, links and linkWeights. 
	 */
	private static void pageRank(Network network, double alpha) {

	    System.out.println ("Starting PageRank computation...");

		//set teleport_weight
	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){ 
	    	Node node = iter.next().getValue();
	    	node.teleportWeight = node.nodeWeight / network.totalNodeWeight;	    	
	    }
	    	
		//double alpha = 0.15; // teleportation probability, we take it as a parameter, but default 0.15
		double beta = 1.0-alpha; // probability to take normal step
		int Ndanglings = 0;      //number of dangling nodes
		Vector<Integer> danglings = new Vector<Integer>(); //keep the id of danglingNodes
				
		//****initiate
		// Take care of dangling nodes, normalize outLinks, and calculate total teleport weight
	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Node node = iter.next().getValue();
	    	if(node.outLinks.isEmpty()){ //&& (node[i]->selfLink <= 0.0)
	    		danglings.add(node.id);
	    		Ndanglings++;
	    	}else{// Normalize the weights	    		
	    		double sum = 0.0; //double sum = node->selfLink; // Take care of self-links ??
	    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ) 
	    			sum += iter2.next().getValue();
	    				      
	    		//node[i]->selfLink /= sum;
	    		for (Iterator<Map.Entry<Integer, Double>> iter3 = node.outLinks.entrySet().iterator(); iter3.hasNext(); ){
	    			Integer nodeId = iter3.next().getKey();
	    			node.outLinks.put(nodeId, node.outLinks.get(nodeId) / sum);
	    		}
		    }
		}
				
		//*********infoMap's eigenvector() function starts from here
		Map<Integer, Double> size_tmp = new HashMap<Integer, Double>(); //initialize it with 1.0/numNode
		double initialSize = 1.0 / network.numNode;
	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) 
	    	size_tmp.put(iter.next().getKey(), initialSize);
	    		
		int Niterations = 0;
		double danglingSize;
		double sqdiff = 1.0;
		double sqdiff_old;
		double sum;
		
		do{
			
			// Calculate dangling size
			danglingSize = 0.0;
		    for(int i=0;i<Ndanglings;i++){
		    	danglingSize += size_tmp.get(danglings.get(i));
		    }
			
		    // Flow from teleportation
		    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
		    	Node node = iter.next().getValue();
		    	node.size = (alpha + beta*danglingSize)*node.teleportWeight;
		    }
		    	
		    // Flow from network steps
		    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
		    	Node node = iter.next().getValue();
			    //node[i]->size += beta*node[i]->selfLink*size_tmp[i]; //selflink!!!
			    for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
			    	Map.Entry<Integer, Double> entry = iter2.next();
			    	network.nodeMap.get(entry.getKey()).size += beta*entry.getValue()*size_tmp.get(node.id);
			    }
		    }		    

		    // Normalize
		    sum = 0.0;
		    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
		    	sum += iter.next().getValue().size;
		    }
		    
		    sqdiff_old = sqdiff;
		    sqdiff = 0.0;
		    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ){
		    	Node node = iter.next().getValue();
		    	node.size /= sum; 
		    	sqdiff += Math.abs(node.size - size_tmp.get(node.id));
		    	size_tmp.put(node.id, node.size);		    	
		    }		    
		    Niterations++;
		    
		    if(sqdiff == sqdiff_old){  //fprintf(stderr,"\n1.0e-10 added to alpha for convergence (precision error)\n");
		      alpha += 1.0e-10;
		      beta = 1.0-alpha;
		    }
		    System.out.println ("Iteration: "+Niterations+ "  sqdiff=" + sqdiff);
		}while((Niterations < 200) && (sqdiff > 1.0e-15 || Niterations < 50));
		
	    System.out.println ("PageRank computation done! The error is " + sqdiff + " after " + Niterations + " iterations.");

		danglingSize = 0.0;
	    for(int i=0;i<Ndanglings;i++){
	    	danglingSize += size_tmp.get(danglings.get(i));
	    }
		  
	}	

	/**
	 * This method computes the betweenness centrality of each node within the network.
	 * It implements the algorithm of (Ulrik Brandes, A Faster Algorithm for Betweenness Centrality. Journal of Mathematical Sociology 25(2):163-177, 2001.)
	 * Also, inspired by the JUNG java implementation of BetweennessCentrality (http://jung.sourceforge.net)
	 * As explained in the above paper, it runs in O(nm) and O(nm + n^2 log n) time on unweighted and weighted networks, respectively, 
	 * where n is the number of nodes and m is the number of links.
	 * Do we need to get a parameter which specifies whether the network is weighted or unweighted???
	 * For example: our PDB primary citation network (~2M nodes, ~20M links) is unweighted, and its journal citation network is weighted (~10K nodes, 1.3M links). 
	 * @param network
	 */
	private static void betweennessCentrality(Network network) {
		
		//this map keeps the list of nodes that are changed during an iteration, at the end of each iteration, we only initialize them, not the whole 2 million dataset 
		Map<Integer, Node> changed = new HashMap<Integer, Node>();
		int changed_cnt = 0; int cnt_node=0;
	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Node s = iter.next().getValue();
	    	cnt_node++;
	    	//initialization
	    	Deque<Node> stack = new ArrayDeque<Node>();
	    	Queue<Node> queue = new LinkedList<Node>();	    	
	    	
			s.numSPs = 1.0;
			s.distance = 0.0;	    	
	    	queue.add(s);
	    	changed.put(s.id, s);
	    	
	    	while (!queue.isEmpty()) {
	    		Node v = queue.remove();
                stack.push(v);
                
			    for (Iterator<Map.Entry<Integer, Double>> iter2 = v.outLinks.entrySet().iterator(); iter2.hasNext(); ){			    	
			    	Node w = network.nodeMap.get(iter2.next().getKey());
			    	//path discovery: w found for the first time
			    	if (w.distance < 0.0){
			    		w.distance = v.distance + 1.0;			    		
			    		queue.add(w);
			    		changed.put(w.id, w);
			    	}
			    	//path counting: edge(v,w) on a shortest path?
			    	if (w.distance == v.distance+1.0){
			    		w.numSPs += v.numSPs;	
			    		w.predecessors.add(v);
			    		changed.put(w.id, w);			    		
			    	}			    	
			    }                
	    	}
	    	
	    	//accumulation - back-propagation of dependencies
		    while (!stack.isEmpty()) {
	    		Node w = stack.pop();
	    		
	    		for (Iterator<Node> iter3 = w.predecessors.iterator(); iter3.hasNext();) {
                    Node v = iter3.next();
                    v.dependency += (v.numSPs / w.numSPs) * (1.0 + w.dependency);     
                    changed.put(v.id, v);
	    		}
	    		
                if (!w.equals(s)) {                	
                	w.betweennessCentrality += w.dependency;                	                	 
                }
	    	}	
		    
		    System.out.println("Computing " + s.name);
		    for (Iterator<Map.Entry<Integer, Node>> iter2 = network.nodeMap.entrySet().iterator(); iter2.hasNext(); ){		    	
		    	Node tmp = iter2.next().getValue();
		    	System.out.println("   " + tmp.name + "  BWC:" + tmp.betweennessCentrality +"  SP:"+tmp.numSPs);
		    }
		    System.out.println("----------------------------");
		    
		    changed_cnt += changed.size();
	    	for (Iterator<Map.Entry<Integer, Node>> iterInit = changed.entrySet().iterator(); iterInit.hasNext(); ){
	    		Node nodeInit = iterInit.next().getValue();	    		
	    		nodeInit.dependency = 0.0;
	    		nodeInit.numSPs = 0.0;
	    		nodeInit.distance = -1.0;
	    		nodeInit.predecessors = new ArrayList<Node>();	    		
	    	}	    	
	    	changed.clear();
	    }
	    
	    System.out.println(changed_cnt);
	}
	
	/**
	 * This method writes the network measures/metrics of each node to an output file.
	 * We use double pipe "||" as a delimiter, because single pipe "|" exist in some of the titles.
	 * @param network: The network object which contains the metrics of each node. 
	 * @param outFileName: The name of the output file.
	 * @throws IOException
	 */
	private static void writeResultsToFile(Network network, String outFileName) throws IOException{
		
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("* NodeId || InDegree(CiteCnt) || OutDegree(RefCnt) || pageRank || nodeName" ); out.newLine(); //|| pageRankNormalized || betweenness       
        String sep = " || "; //separator

	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
	    	Node node = iter.next().getValue();
	    	
	    	out.write(node.id + sep + node.inLinks.size() + sep + node.outLinks.size() + sep + node.size + sep + node.betweennessCentrality + sep + node.numSPs + sep+ node.name); //node.size * network.numLink + sep + node.betweennessCentrality + sep  
	    	out.newLine();
	    }                        
        out.flush();       
        out.close();            
	}
}

/**
 * A utility class to hold network attributes: 
 */
class Network {
	
	String name;  //network file name
	int numNode;  //number of nodes
	int numLink;  //number of links
	double totalNodeWeight = 0.0;

	Map<Integer, Node> nodeMap = new HashMap<Integer, Node>(); //key:nodeId value:nodeName

	Network(){}
	Network(String p_name){
		name = p_name;
	}	
}

/**
 * A utility class to hold node attributes.
 */
class Node {
	
	Boolean visited = false;
	Integer id; 
	String name;	 
	double nodeWeight; //initial nodeWeight, if not given, default 1.0
	Map<Integer, Double> inLinks = new HashMap<Integer, Double>();  // neighborNodeId - linkWeight, default linkWeight = 1.0
	Map<Integer, Double> outLinks = new HashMap<Integer, Double>(); // neighborNodeId - linkWeight, default linkWeight = 1.0
		
	//pageRank related attributes
	double teleportWeight = 0.0;
	double size = 0.0; // stationary distribution of the node. is it also pageRank? need to think...
	
	//betweenness Centrality related attributes
	double distance = -1.0; //default distance -1
	double numSPs = 0.0; // number of shortestPaths, default 0
	double dependency = 0.0; //default 0
	ArrayList<Node> predecessors = new ArrayList<Node>();
	double betweennessCentrality = 0.0; //default 0
	
	//ranking, these are the ranking of the nodes within the network from 1 to n in descending order.
	int citeCountRank=0;
	int pageRank=0;
	int betweennessCentralityRank=0;
	
	Node(){}
	Node(Integer p_id, String p_name, double p_nodeWeight){
		id = p_id;
		name = p_name;
		nodeWeight = p_nodeWeight;
	}		
}
class YearCitations {	
	String year; 
	Double totalCitationCount;
	Double numOfPapers;
	Double numOfDanglingPapers;

	YearCitations(){ }
	YearCitations(String p_year, Double p_totalCitationCount, Double p_numOfPapers, Double p_numOfDanglingPapers){
		year = p_year;
		totalCitationCount = p_totalCitationCount;
		numOfPapers = p_numOfPapers;
		numOfDanglingPapers = p_numOfDanglingPapers;
    }
}
