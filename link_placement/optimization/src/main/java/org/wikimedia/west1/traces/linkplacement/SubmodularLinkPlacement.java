package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class SubmodularLinkPlacement extends LinkPlacement {

	public SubmodularLinkPlacement(String datadir) throws IOException {
		super(datadir);
	}

	private Set<LinkCandidate> neighborsInSolution(LinkCandidate cand) {
		Set<LinkCandidate> result = new HashSet<LinkCandidate>();
		for (int tree : candsToTrees.get(cand)) {
			for (LinkCandidate neighbor : treesToCands.get(tree)) {
				if (solution.contains(neighbor)) {
					result.add(neighbor);
				}
			}
		}
		return result;
	}

	@Override
	public void reset() {
		solution = new HashSet<LinkCandidate>();
		priorityQueue = new PriorityQueue<LinkCandidate>();
		initMargGains();
	}

	@Override
	public void placeLinks(int numLinks, boolean quiet) {
		// Pick the numLinks optimal candidates greedily.
		for (int i = 0; i < numLinks; ++i) {
			// Get candidates from the priority queue until we find an uncorrupted one.
			while (true) {
				// Pick the candidate with the largest marginal gain (which may be corrupted).
				LinkCandidate cand = priorityQueue.poll();
				// If cand's marginal gain is corrupted, recompute it, mark cand as uncorrupted, and
				// reinsert it into the priority queue.
				if (cand.corrupted) {
					float oldMargGain = cand.margGain;
					cand.margGain = margGain(cand);
					cand.corrupted = false;
					priorityQueue.offer(cand);
					if (!quiet) {
						System.out.format(
						    "### CORRPUPT: %s, score: %s, old gain: %s, new gain: %s, neigh: %s\n", cand.name,
						    cand.score, oldMargGain, cand.margGain, neighborsInSolution(cand).toString());
					}
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
					if (!quiet) {
						System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name,
						    cand.score, cand.margGain, candsToTrees.get(cand).length);
					}
					solution.add(cand);
					break;
				}
			}
		}
	}

	public float getUpperBound(int numLinks, float solutionValue) {
		float bound = solutionValue;
		for (int i = 0; i < numLinks; ++i) {
			// Get candidates from the priority queue until we find an uncorrupted one.
			while (true) {
				// Pick the candidate with the largest marginal gain (which may be corrupted).
				LinkCandidate cand = priorityQueue.poll();
				// If cand's marginal gain is corrupted, recompute it and mark cand as uncorrupted.
				if (cand.corrupted) {
					cand.margGain = margGain(cand);
					cand.corrupted = false;
					priorityQueue.offer(cand);
				}
				// If cand's marginal gain is uncorrupted, cand is the optimal next greedy choice.
				else {
					bound += cand.margGain;
					break;
				}
			}
		}
		return bound;
	}

}
