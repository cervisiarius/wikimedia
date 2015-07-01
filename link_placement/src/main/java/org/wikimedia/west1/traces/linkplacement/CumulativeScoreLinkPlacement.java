package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;

public class CumulativeScoreLinkPlacement extends LinkPlacement {

	public CumulativeScoreLinkPlacement(String datadir) throws IOException {
		super(datadir);
	}

	@Override
	public void reset() {
		solution = new HashSet<LinkCandidate>();
		priorityQueue = new PriorityQueue<LinkCandidate>();
		initMargGains();
	}

	@Override
	public void placeLinks(int numLinks, boolean quiet) {
		// Pick the numLinks optimal candidates greedily, without updating marginal gains.
		for (int i = 0; i < numLinks; ++i) {
			// Pick the candidate with the largest (initial) marginal gain.
			LinkCandidate cand = priorityQueue.poll();
			solution.add(cand);
			if (!quiet) {
				System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name, cand.score,
				    cand.margGain, candsToTrees.get(cand).length);
			}
		}
	}

}
