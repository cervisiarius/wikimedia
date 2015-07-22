package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TreeModelLinkPlacement extends LinkPlacement {

	public TreeModelLinkPlacement(String datadir) throws IOException {
		super(datadir);
	}

	@Override
	protected double getWeight(String src) {
		return srcCounts.get(src);
	}

	public double evaluate() {
		Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
		double value = 0;
		for (String src : bySource.keySet()) {
			double probNone = 1;
			for (LinkCandidate cand : bySource.get(src)) {
				probNone *= (1 - cand.score);
			}
			// System.out.format("%s = %s * (1 - %s)\n", srcCounts.get(src) * (1 - probNone),
			// srcCounts.get(src), probNone);
			value += getWeight(src) * (1 - probNone);
		}
		return value;
	}

	@Override
	protected double computeScore(Map<String, Double> values) {
		return values.get("pst_indirect");
	}

	@Override
	protected void initMargGains() {
		int s = 0;
		for (String src : srcToCands.keySet()) {
			if (++s % 1000 == 0)
				System.err.format("Source %d of %s\n", s, srcToCands.size());
			List<LinkCandidate> cands = srcToCands.get(src);
			// Sort all cands by score.
			Collections.sort(cands, new LinkCandidate.ScoreComparator());
			double probNone = 1;
			double prevValue = 0;
			for (LinkCandidate cand : cands) {
				probNone *= (1 - cand.score);
				double value = getWeight(src) * (1 - probNone);
				cand.margGain = value - prevValue;
				priorityQueue.offer(cand);
				prevValue = value;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String dir;
		try {
			dir = args[0];
		} catch (ArrayIndexOutOfBoundsException e) {
			// dir = LinkPlacement.DATADIR_WIKIPEDIA;
			dir = LinkPlacement.DATADIR_SIMTK;
		}
		TreeModelLinkPlacement obj = new TreeModelLinkPlacement(dir);
		obj.placeLinks((int) 1e5, new PrintStream(dir + "/link_placement_results_COINS-PAGE.tsv"));
	}

}
