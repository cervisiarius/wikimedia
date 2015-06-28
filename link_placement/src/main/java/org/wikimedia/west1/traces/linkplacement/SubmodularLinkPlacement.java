package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SubmodularLinkPlacement extends LinkPlacement {

	public SubmodularLinkPlacement(int numLinks) throws IOException {
		super(numLinks);
		initMargGains();
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
	public void placeLinks() {
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
					System.out.format("### CORRPUPT: %s, score: %s, old gain: %s, new gain: %s, neigh: %s\n",
					    cand.name, cand.score, oldMargGain, cand.margGain, neighborsInSolution(cand)
					        .toString());
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
					System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name, cand.score,
					    cand.margGain, candsToTrees.get(cand).length);
					solution.add(cand);
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		SubmodularLinkPlacement placement = new SubmodularLinkPlacement(100);
		System.err.println("DONE LOADING DATA");
		placement.placeLinks();
		System.err.format("Value: %s", placement.evaluate());
	}

}
