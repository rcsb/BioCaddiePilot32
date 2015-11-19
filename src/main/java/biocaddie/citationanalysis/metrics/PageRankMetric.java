package biocaddie.citationanalysis.metrics;

import java.io.IOException;

import org.apache.spark.SparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.spark.util.SparkUtils;

public class PageRankMetric {
	
	public static void main(String[] args) {
			// Set up contexts.
			long start = System.nanoTime();
			
			SparkContext sc = SparkUtils.getSparkContext();
			
			Graph<Object, Object> graph = GraphLoader.edgeListFile(sc, args[0], false, 0, StorageLevel.MEMORY_AND_DISK(), StorageLevel.MEMORY_AND_DISK());
//			PageRank.runUntilConvergence(graph, 1E-15, 0.5, Integer.class, Integer.class);
	
	}
//	// Load the edges as a graph
//	val graph = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")
//	// Run PageRank
//	val ranks = graph.pageRank(0.0001).vertices
//	// Join the ranks with the usernames
//	val users = sc.textFile("graphx/data/users.txt").map { line =>
//	  val fields = line.split(",")
//	  (fields(0).toLong, fields(1))
//	}
//	val ranksByUsername = users.join(ranks).map {
//	  case (id, (username, rank)) => (username, rank)
//	}
//	// Print the result
//	println(ranksByUsername.collect().mkString("\n"))
}
