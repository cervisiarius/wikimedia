package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;

public class ScoreLinkPlacement extends LinkPlacement {

	public ScoreLinkPlacement(String datadir) throws IOException {
		super(datadir);
	}

	@Override
	public void reset() {
		solution = new HashSet<LinkCandidate>();
		priorityQueue = new PriorityQueue<LinkCandidate>();
		for (LinkCandidate cand : candsToTrees.keySet()) {
			// Set the margGain field, which is used for comparison, to score.
			cand.margGain = cand.score;
		}
		for (LinkCandidate cand : candsToTrees.keySet()) {
			priorityQueue.offer(cand);
		}
	}

	@Override
	public void placeLinks(int numLinks, boolean quiet) {
		// Pick the numLinks optimal candidates greedily, without updating marginal gains.
		for (int i = 0; i < numLinks; ++i) {
			// Pick the candidate with the largest (initial) marginal gain.
			LinkCandidate cand = priorityQueue.poll();
			if (!quiet) {
				System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name, cand.score,
				    cand.margGain, candsToTrees.get(cand).length);
			}
			solution.add(cand);
		}
	}

}
