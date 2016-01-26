package org.biocaddie.citationanalysis.network.fast;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Currently this class has three features:
 * 1-) Converts paper citation network to paper co-citation network
 * 2-) Converts journal citation network to clean_journal_citation network by 
 * 		- excluding self-journal citations, 
 * 		- excluding four major interdisciplinary journals (Nature, Science, PNAS, PlosOne)
 * 		- excluding journals which make$receive citations less than 1
 * 3-) Prints network summary (global properties of the network) 
 */
public class NetworkUtilsFast {
    final static Charset ENCODING = StandardCharsets.UTF_8;
    final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	public static void main(String[] args) throws IOException {

		if(args.length < 2){
    		System.out.println("Call: java org.biocaddie.citationanalysis.network.NetworkUtils <flag: 1, 2 or 3> <network.net>");
    		System.out.println("1: Converts paper citation network to paper co-citation network");
    		System.out.println("2: Converts journal citation network to clean journal citation network");
    		System.out.println("3: Prints the network summary (global properties) of a network");
    		System.exit(1);
    	}

		String inputFileName = args[1];		
		String fileSeparator = System.getProperty("file.separator");		
		String pathToFile = inputFileName.substring(0, inputFileName.lastIndexOf(fileSeparator)+1);		
	
		NetworkFast network = new NetworkFast(inputFileName);		
		readPajekNetFile(network); 

		int operation = Integer.valueOf(args[0]).intValue();			
		if (operation == 1){ //convert to paper co-citation network
			System.out.println("Converting paper citation network to paper co-citation network...");
			String outFileName = inputFileName.substring(inputFileName.lastIndexOf(fileSeparator)+1, inputFileName.lastIndexOf(".")) + "_cocitation.net";		
// TODO			Network cocitationNetwork = convertCitationToCocitation(network, pathToFile+outFileName);
// TODO			network = cocitationNetwork;					
		}else if (operation == 2){ //clean journal citation network
			System.out.println("Converting journal citation network to clean journal citation network...");			
			String outFileName = inputFileName.substring(inputFileName.lastIndexOf(fileSeparator)+1, inputFileName.lastIndexOf(".")) + "_clean.net";	
// TODO			generateCleanJournalNetwork(network, pathToFile+outFileName);			
		}else if (operation == 3){ //print network summary
// TODO			printNetworkSummary(network);			
		}else {
			System.out.println("!!!Flag should be 1, 2 or 3.");
			System.exit(1);			
		}
		System.out.println("DONE...");
	}

	public static void printNetworkSummary(NetworkFast network) {
		
			int noInlinks = 0;
			int noOutLinks = 0;
			int noInOutLinks = 0;
			int inOutLinks = 0; int selfLink = 0; int loopLink = 0; int totIn=0; int totOut=0; int selflinkWeight = 0; int totInLinkSize = 0;
//			for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
			for (NodeFast node: network.nodeMap.values()) {	
	    		
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
	    		}

//	    		//citation loop (A->B and B->A)
//	    		for (Iterator<Map.Entry<Integer, Double>> iterInLinks = node.inLinks.entrySet().iterator(); iterInLinks.hasNext(); ) {
				for (Int2DoubleMap.Entry entry: node.inLinks.int2DoubleEntrySet()) {
//	    			Entry<Integer, Double> entry = iterInLinks.next(); 
	    			int other_id = entry.getIntKey();
	    			double weight = entry.getDoubleValue();
	    			totInLinkSize += weight;
	    			if (network.nodeMap.get(other_id).inLinks.containsKey(node.id)){
	    				loopLink++;
	    			}
	    			if (node.id == other_id)
	    				selflinkWeight += weight;
	    		}
	    		
	    		totIn += node.inLinks.size();
	    		totOut += node.outLinks.size();
	    	}

			System.out.println("Number of nodes with noInlinks(citation) : " + noInlinks);
			System.out.println("Number of nodes with noOutLinks(reference): " + noOutLinks);
			System.out.println("Number of nodes with noInLinks & noOutLinks: " + noInOutLinks);
			System.out.println("Number of nodes with at least one inLink and one outLink: " + inOutLinks);
			System.out.println("Number of nodes with selfLink: " + selfLink);
			System.out.println("Total selflink weight: " + selflinkWeight);
			System.out.println("Number of nodes with Loop (A->B and B->A): " + loopLink);
			System.out.println("Number of total outLinks: " + totOut);
			System.out.println("Number of total inLinks: " + totIn);
			System.out.println("Total inLinks Size: " + totInLinkSize);
			System.out.println("Number of nodes within the network: " + network.nodeMap.size());
	}
	
	/**
	 * This method converts the citation network to co-citation network by taking the combinations of each paper's outLinks.
	 * Co-citation network will be undirected and weighted. 
	 * @param network
	 * @param newFileName
	 * @return
	 * @throws IOException
	 */
	public static NetworkFast convertCitationToCocitation(NetworkFast network, String newFileName) throws IOException {
		//create new network for co-citation, since it is undirected use only outLinks, inLinks will be always empty
		NetworkFast coCitationNetwork = new NetworkFast(newFileName);		
				
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		NodeFast node = iter.next().getValue();
    		if(node.outLinks.size() < 2 ) //then there is no combination, in order to find co-citation, outLink >=2 
    			continue;

    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
    			Integer otherId = iter2.next().getKey();
    			NodeFast otherNode = network.nodeMap.get(otherId);
    			if (!coCitationNetwork.nodeMap.containsKey(otherId)){
    				NodeFast newNode = new NodeFast(otherNode.id, otherNode.name, otherNode.nodeWeight); 
    				coCitationNetwork.nodeMap.put(otherId, newNode);    			
    			}    			
    		}     			

    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ){
    			NodeFast cociteNode1 = coCitationNetwork.nodeMap.get(iter2.next().getKey());
    			for (Iterator<Map.Entry<Integer, Double>> iter3 = node.outLinks.entrySet().iterator(); iter3.hasNext(); ){
        			NodeFast cociteNode2 = coCitationNetwork.nodeMap.get(iter3.next().getKey());
        			if (cociteNode1.id == cociteNode2.id)
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
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = coCitationNetwork.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	NodeFast jNode = iter.next().getValue();
	    	jNode.id = id;
	    	out.write(id + " \"" + jNode.name+"\""); out.newLine();
	    	numLinks = numLinks + jNode.outLinks.size();
	    	id++;
	    }
		
	    out.write("*Edges " + numLinks); out.newLine();	
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = coCitationNetwork.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	NodeFast jNode = iter.next().getValue();
	    	Integer from = jNode.id;
	    		    	
		    for (Iterator<Map.Entry<Integer, Double>> iter2 = jNode.outLinks.entrySet().iterator(); iter2.hasNext(); ) {	    		    	
		    	Map.Entry<Integer, Double> entry2 = iter2.next();
		    	Integer to = coCitationNetwork.nodeMap.get(entry2.getKey()).id;
		    	String weight = entry2.getValue().toString();
		    	out.write(from + " " + to + " " + weight); out.newLine();	    		
	    	}	    	
	    }		
        out.flush();       
        out.close();    		
		
		return coCitationNetwork;
	}
	
	/**
	 * Since we are interested in information flow between journals,
	 * 1-) we exclude self-citations
	 * 2-) we exclude journals which are single receive citations < 1 && make citations < 1
	 * 3-) we exclude four interdisciplinary journals: Nature, Science, PNAS and Plos One
	 */
	public static void generateCleanJournalNetwork(NetworkFast network, String newFileName) throws IOException {		
		
		//exclude four major interdisciplinary journals: Nature(0410462), Science(0404511), PNAS(7505876)  and PlosOne (101285081)
		List<Integer> numbers = new ArrayList<>();
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		NodeFast node = iter.next().getValue();
    		if (node.name.contains("|| 0410462") || node.name.contains("|| 0404511") || node.name.contains("|| 7505876") || node.name.contains("|| 101285081"))
    			numbers.add(node.id);
		}
		for (Integer i : numbers) {
			NodeFast journal = network.nodeMap.get(i);
			for (Iterator<Map.Entry<Integer, Double>> iter = journal.outLinks.entrySet().iterator(); iter.hasNext(); ) 
				network.nodeMap.get(iter.next().getKey()).inLinks.remove(journal.id);
			for (Iterator<Map.Entry<Integer, Double>> iter = journal.inLinks.entrySet().iterator(); iter.hasNext(); ) 
				network.nodeMap.get(iter.next().getKey()).outLinks.remove(journal.id);
			network.nodeMap.remove(i);		    
		}
				
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
    		NodeFast node = iter.next().getValue();
    		  		
    		//remove self-links
    		if (node.inLinks.containsKey(node.id))
    			node.inLinks.remove(node.id);    		
    		if (node.outLinks.containsKey(node.id))
    			node.outLinks.remove(node.id);

    		//exclude journals which receive or make citations <= 1
    		int outWeight = 0; int inWeight = 0;    		
    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.inLinks.entrySet().iterator(); iter2.hasNext(); )
    			inWeight += iter2.next().getValue();        		
    		
    		for (Iterator<Map.Entry<Integer, Double>> iter2 = node.outLinks.entrySet().iterator(); iter2.hasNext(); ) 
    			outWeight += iter2.next().getValue();
		
   		    		
    		if (outWeight < 1 || inWeight < 1){
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
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	NodeFast jNode = iter.next().getValue();
	    	if (jNode.inLinks.size() == 0 && jNode.outLinks.size() == 0 ) //do not include single nodes
	    		continue;
	    	jNode.id = id;
	    	out.write(id + " \"" + jNode.name+"\""); out.newLine();
	    	numLinks = numLinks + jNode.inLinks.size();
	    	id++;
	    }
		
	    out.write("*Arcs " + numLinks); out.newLine();	
		for (Iterator<Map.Entry<Integer, NodeFast>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {				
	    	NodeFast jNode = iter.next().getValue();
	    	Integer from = jNode.id;
	    		    	
		    for (Iterator<Map.Entry<Integer, Double>> iter2 = jNode.outLinks.entrySet().iterator(); iter2.hasNext(); ) {	    		    	
		    	Map.Entry<Integer, Double> entry2 = iter2.next();
		    	Integer to = network.nodeMap.get(entry2.getKey()).id;
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
	 * @param network: network object which contains the inputFileName
	 * @throws IOException
	 */
	public static void readPajekNetFile(NetworkFast network) throws IOException {
		
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
	    	    
	    	    int id = Integer.valueOf(line.substring(0, nameStart-1));	    	    
	    	    String name = line.substring(nameStart+1, nameEnd);
	    	    
	    	    String nodeWeight_str = line.substring(nameEnd+1).trim();
	    	    double nodeWeight = 1.0; //default value
	    	    if (nodeWeight_str.length() > 0 && Double.valueOf(nodeWeight_str) > 0.0)
	    	    	nodeWeight = Double.valueOf(nodeWeight_str);	    	    
	    	    	
	    	    NodeFast node = new NodeFast(id, name, nodeWeight);
	    	    network.nodeMap.put(id, node);
	    	    network.totalNodeWeight += nodeWeight;
	    	}
	    	
	    	//Reading links
	    	if (currentPart.equals("links")){	    	

	    		String[] tokens = line.split(" ");
	    		int from = Integer.valueOf(tokens[0].trim());
	    	    int to = Integer.valueOf(tokens[1].trim());
	    		if (!network.nodeMap.containsKey(from) || !network.nodeMap.containsKey(to))
	    			continue;
	    		
	    		double weight = 1.0; //default value 	    				    				
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
	 * this method try to find the strongly connected components of a network, but currently it is incomplete
	 * @param network
	 * @param startNodeId
	 * @throws IOException
	 */
	public static void stronglyConnectedComponents(NetworkFast network, Integer startNodeId) throws IOException {
		Queue<NodeFast> queue = new LinkedList<NodeFast>();	    	
		int numOfVisitedNodes = 0;
		NodeFast startNode = network.nodeMap.get(startNodeId);    	
		queue.add(startNode);  
		startNode.visited = true; 
		numOfVisitedNodes++;		    	
		while (!queue.isEmpty()) {
			NodeFast v = queue.remove();

		    //assume undirected, use both inLinks and outLinks
			for (Iterator<Map.Entry<Integer, Double>> iter = v.outLinks.entrySet().iterator(); iter.hasNext(); ){			    	
				NodeFast w = network.nodeMap.get(iter.next().getKey());
			   	if (!w.visited){
			   		queue.add(w); 
			   		w.visited = true;
			   		numOfVisitedNodes++;
			   	}
			}
			
			for (Iterator<Map.Entry<Integer, Double>> iter = v.inLinks.entrySet().iterator(); iter.hasNext(); ){
			   	NodeFast w = network.nodeMap.get(iter.next().getKey());
			   	if (!w.visited){
			   		queue.add(w); 
			   		w.visited = true;
			   		numOfVisitedNodes++;
			   	}
			}
	    }		    	
	    System.out.println("size: " + network.nodeMap.size());
	    System.out.println("numOfVisitedNodes: " + numOfVisitedNodes);
	}	
}
