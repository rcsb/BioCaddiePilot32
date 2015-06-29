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
		if(args.length < 2){
    		System.out.println("Call: java org.biocaddie.citationanalysis.network.NetworkMeasuresMetrics <flag> <network.net> <optional pageRankDampingFactor, default:0.5>");
    		System.out.println("flag values= 1:pageRank 2:betweenness, 12: both pageRank and betweenness");
    		System.exit(1);
    	}
		
		int flag = Integer.valueOf(args[0]).intValue();
		String inputFileName = args[1];		
		String outFileName = inputFileName.substring(0, inputFileName.lastIndexOf("."))+"_metrics.txt";		
				
		System.out.println("Start computing network measures/metrics...");
		Network network = new Network(inputFileName);		
		System.out.println("Start Time: " + dateFormat.format(new Date()));
				
		//Step 1: Read the network in Pajek.net file format
		NetworkUtils.readPajekNetFile(network); 
				
		//Step 2: Compute PageRank of each node within the network
		double alpha = 0.5; // default value for teleportation probability
		if (args.length > 2)
			alpha = Double.valueOf(args[2]);		
		if (flag == 1 || flag == 12)
			pageRank(network, alpha);
		
		//Step 3: Compute the Betweenness centrality of each node within the network
		if (flag == 2 || flag == 12)
			betweennessCentrality(network);
		
		//Step 4: Write the measures/metrics of each node to a file // Node Id | InDegreeOrCiteCount | PageRank | BetweennessCentrality		
		writeResultsToFile(network, outFileName, alpha);
		
		System.out.println("End Time  : " + dateFormat.format(new Date()));	    		
		System.out.println("Done!..");
	}
	
	
	/**
	 * Following the Greedy::eigenvector(void) implementation of InfoMap written in C++ by Martin Rosvall. http://www.tp.umu.se/~rosvall/code.html
	 * This method computes the pageRank of each node within the network using the Power method.
	 * Power method iterates while((Niterations < 200) && (sqdiff > 1.0e-15 || Niterations < 50))
	 * @param network: The network object which contains the nodes, links and linkWeights. 
	 */
	private static void pageRank(Network network, double alpha) {
	    System.out.println ("Starting PageRank computation with teleportation probability: "+alpha + "  "+ dateFormat.format(new Date()));

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
		
	    System.out.println ("PageRank computation done! The error is " + sqdiff + " after " + Niterations + " iterations.  "+ dateFormat.format(new Date()));

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
	 * This implementation computes betweennessCentrality in an unweighted manner.
	 * @param network
	 */
	private static void betweennessCentrality(Network network) {
	    System.out.println ("Starting BetweennessCentrality computation... " + dateFormat.format(new Date()));

	    //this map keeps the list of nodes that are changed during an iteration, at the end of each iteration, we only initialize them, not the whole 2 million dataset 
		Map<Integer, Node> changed = new HashMap<Integer, Node>();
	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {
	    	Node s = iter.next().getValue();
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
		    
	    	for (Iterator<Map.Entry<Integer, Node>> iterInit = changed.entrySet().iterator(); iterInit.hasNext(); ){
	    		Node nodeInit = iterInit.next().getValue();	    		
	    		nodeInit.dependency = 0.0;
	    		nodeInit.numSPs = 0.0;
	    		nodeInit.distance = -1.0;
	    		nodeInit.predecessors = new ArrayList<Node>();	    		
	    	}	    	
	    	changed.clear();
	    }	    
	    System.out.println ("BetweennessCentrality computation done... " + dateFormat.format(new Date()));
	}
	
	/**
	 * This method writes the network measures/metrics of each node to an output file.
	 * We use double pipe "||" as a delimiter, because single pipe "|" exist in some of the titles.
	 * @param network: The network object which contains the metrics of each node. 
	 * @param outFileName: The name of the output file.
	 * @throws IOException
	 */
	private static void writeResultsToFile(Network network, String outFileName, double alpha) throws IOException{
		
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(outFileName)));
        out.write("* NodeId || InDegree(CiteCnt) || OutDegree(RefCnt) || pageRank("+alpha+")  || betweennessCentrality || nodeName" ); out.newLine(); //|| pageRankNormalized || betweenness       
        String sep = " || "; //separator

	    for (Iterator<Map.Entry<Integer, Node>> iter = network.nodeMap.entrySet().iterator(); iter.hasNext(); ) {	
	    	Node node = iter.next().getValue();	    	
	    	out.write(node.id + sep + node.inLinks.size() + sep + node.outLinks.size() + sep + node.size + sep + node.betweennessCentrality + sep+ node.name);   
	    	out.newLine();
	    }                        
        out.flush();       
        out.close();            
	}
}


