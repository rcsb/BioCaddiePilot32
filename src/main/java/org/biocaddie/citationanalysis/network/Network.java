package org.biocaddie.citationanalysis.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class to hold network attributes: 
 */
public class Network {
	
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
