package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CumulativeScoreLinkPlacement extends LinkPlacement {

  public CumulativeScoreLinkPlacement(String datadir, ScoreType scoreType) throws IOException {
    super(datadir, scoreType);
  }

  // Evaluate under the chain-model objective.
  public double evaluate(Set<LinkCandidate> solution) {
    Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
    double value = 0;
    for (String src : bySource.keySet()) {
      double srcValue = 0;
      for (LinkCandidate cand : bySource.get(src)) {
        srcValue += cand.score;
      }
      srcValue = srcCounts.get(src) * srcValue / (srcValue + srcExistingLinkScoreSum.get(src));
      value += srcValue;
    }
    return value;
  }

  public void placeLinks(int numLinks, boolean quiet) {
    // Use as the value of a candidate its score times its source's count. Use this value as the
    // marginal gain (although it's not), since the priority queue sorts candidates by marginal
    // gain.
    for (String src : srcToCands.keySet()) {
      for (LinkCandidate cand : srcToCands.get(src)) {
        cand.margGain = srcCounts.get(src) * cand.score;
        priorityQueue.offer(cand);
      }
    }
    // Pick the numLinks optimal candidates greedily.
    for (int i = 0; i < numLinks; ++i) {
      // Pick the candidate with the largest marginal gain.
      LinkCandidate cand = priorityQueue.poll();
      solution.add(cand);
      if (!quiet) {
        System.out.format("(%d) %s, cum_score: %.1f, n_src: %s\n", i, cand, cand.margGain,
            srcCounts.get(cand.src));
      }
    }
  }

  public static void main(String[] args) throws IOException {
    CumulativeScoreLinkPlacement obj = new CumulativeScoreLinkPlacement(
        LinkPlacement.DATADIR_WIKIPEDIA, ScoreType.P_ST);
    obj.placeLinks(100, false);
    System.out.println(obj.evaluate());
  }

}
