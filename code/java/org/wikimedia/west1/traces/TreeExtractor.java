package org.wikimedia.west1.traces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class TreeExtractor {
	
	public static class Node {
		//private long time;
		//private String url;
		//private String referer;
		private int id;
		private List<Node> children = new ArrayList<Node>();
		
		public Node(int id, List<Node> children) {
			this.id = id;
			this.children = children;
		}
	}
	
	public List<Node> getMinSpanningForest(Node root) {
		Map<Node, PriorityQueue<Node>> parents = new HashMap<Node, PriorityQueue<Node>>();
		
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
