package org.wikimedia.west1.traces.linkplacement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public class SubmodularLinkPlacement {

	private int numLinks;
	private Map<LinkCandidate, int[]> candsToTrees;
	private Map<Integer, LinkCandidate[]> treesToCands;
	private Set<LinkCandidate> solution;
	private PriorityQueue<LinkCandidate> priorityQueue;

	public SubmodularLinkPlacement(int numLinks) {
		this.numLinks = numLinks;
		loadTreeData();
		loadCandScores();
		initMargGains();
		solution = new HashSet<LinkCandidate>();
	}

	private void loadTreeData() {
		candsToTrees = new HashMap<LinkCandidate, int[]>();
		treesToCands = new HashMap<Integer, LinkCandidate[]>();
		// TODO
	}

	private void loadCandScores() {
		// TODO
	}

	private void initMargGains() {
		// Compute all marginal gains
		for (LinkCandidate cand : candsToTrees.keySet()) {
			cand.margGain = cand.score;
		}
		priorityQueue = new PriorityQueue<LinkCandidate>();
		for (LinkCandidate cand : candsToTrees.keySet()) {
			priorityQueue.offer(cand);
		}
	}

	private float margGain(LinkCandidate cand) {
		float mg = 0;
		for (int tree : candsToTrees.get(cand)) {
			float inc = cand.score;
			for (LinkCandidate neighbor : treesToCands.get(tree)) {
				if (solution.contains(neighbor)) {
					inc *= (1 - neighbor.score);
				}
			}
			mg += inc;
		}
		return mg;
	}

	private void placeLinks() {
		// Pick the numLinks optimal candidates greedily.
		for (int i = 0; i < numLinks; ++i) {
			// Get candidates from the priority queue until we find an uncorrupted one.
			while (true) {
				// Pick the candidate with the largest marginal gain (which may be corrupted).
				LinkCandidate cand = priorityQueue.poll();
				// If cand's marginal gain is corrupted, recompute it, mark cand as uncorrupted, and
				// reinsert it into the priority queue.
				if (cand.corrupted) {
					cand.margGain = margGain(cand);
					cand.corrupted = false;
					priorityQueue.offer(cand);
				}
				// If cand's marginal gain is uncorrupted, cand is the optimal next greedy choice. Mark all
				// other candidates that appear in trees together with cand as corrupted, add cand to the
				// solution, and break from the while loop.
				else {
					for (int tree : candsToTrees.get(cand)) {
						for (LinkCandidate neighbor : treesToCands.get(tree)) {
							neighbor.corrupted = true;
						}
					}
					solution.add(cand);
					break;
				}
			}
		}
	}

	public static void main(String[] args) {
		SubmodularLinkPlacement placement = new SubmodularLinkPlacement(100);
		placement.placeLinks();
		System.out.println(placement.solution);
	}

}
