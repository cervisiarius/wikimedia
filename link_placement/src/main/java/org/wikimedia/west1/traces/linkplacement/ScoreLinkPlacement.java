package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.util.PriorityQueue;

public class ScoreLinkPlacement extends LinkPlacement {

	public ScoreLinkPlacement(int numLinks, String datadir) throws IOException {
		super(numLinks, datadir);
		for (LinkCandidate cand : candsToTrees.keySet()) {
			// Set the margGain field, which is used for comparison, to score.
			cand.margGain = cand.score;
		}
		priorityQueue = new PriorityQueue<LinkCandidate>();
		for (LinkCandidate cand : candsToTrees.keySet()) {
			priorityQueue.offer(cand);
		}
	}

	@Override
	public void placeLinks() {
		// Pick the numLinks optimal candidates greedily, without updating marginal gains.
		for (int i = 0; i < numLinks; ++i) {
			// Pick the candidate with the largest (initial) marginal gain.
			LinkCandidate cand = priorityQueue.poll();
			System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name, cand.score,
			    cand.margGain, candsToTrees.get(cand).length);
			solution.add(cand);
		}
	}

	public static void main(String[] args) throws IOException {
		ScoreLinkPlacement placement = new ScoreLinkPlacement(100, LinkPlacement.DATADIR_SIMTK);
		System.err.println("DONE LOADING DATA");
		placement.placeLinks();
		System.err.format("Value: %s", placement.evaluate());
	}

}
