package org.wikimedia.west1.traces;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public class TreeExtractor {

	public static class Node {
		// private long time;
		// private String url;
		// private String referer;
		private int id;
		private List<Node> successors = new ArrayList<Node>();
		private PriorityQueue<Node> parents = new PriorityQueue<Node>(256, new NodeComparator());

		public Node(int id, List<Node> successors) {
			this.id = id;
			this.successors = successors;
			for (Node succ : successors) {
				succ.parents.offer(this);
			}
		}

		public static class NodeComparator implements Comparator<Node> {
			@Override
			public int compare(Node a, Node b) {
				// TODO: check if this order is right. use time
				return a.id - b.id;
			}
		}

		public boolean equals(Node other) {
			// TODO: implement
			return this.equals(other);
		}

		public int hashCode() {
			// TODO: implement
			return this.hashCode();
		}

		// Set rather than List because it's a DAG, so several parents can have the same successor,
		// and we want to list it only once.
		public void listSubgraph(Set<Node> result) {
			result.add(this);
			for (Node succ : successors) {
				succ.listSubgraph(result);
			}
		}
	}

	public List<Node> getMinSpanningForest(Node root) {
		Map<Node, List<Node>> children = new HashMap<Node, List<Node>>();
		Set<Node> currentRoots = new HashSet<Node>();
		root.listSubgraph(currentRoots);
		for (Node currentRoot : )

		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
