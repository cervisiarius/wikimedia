package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

public class CumulativeScoreLinkPlacement extends LinkPlacement {

  public CumulativeScoreLinkPlacement(String datadir) throws IOException {
    super(datadir);
  }

  @Override
  protected double computeScore(Map<String, Double> values) {
    return values.get("pst_indirect");
  }

  @Override
  protected void initMargGains() {
    // Use as the value of a candidate its score times its source's count.
    for (String src : srcToCands.keySet()) {
      for (LinkCandidate cand : srcToCands.get(src)) {
        cand.margGain = srcCounts.get(src) * cand.score;
        priorityQueue.offer(cand);
      }
    }
  }

  // Evaluate under the chain-model objective.
  @Override
  public double evaluate() {
    Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
    double value = 0;
    for (String src : bySource.keySet()) {
      double srcNewLinkScoreSum = 0;
      for (LinkCandidate cand : bySource.get(src)) {
        srcNewLinkScoreSum += cand.score;
      }
      value += srcClickCounts.get(src) * srcNewLinkScoreSum
          / (srcNewLinkScoreSum + srcExistingLinkScoreSum.get(src));
    }
    return value;
  }

  public static void main(String[] args) throws IOException {
    String dir;
    try {
      dir = args[0];
    } catch (ArrayIndexOutOfBoundsException e) {
      dir = LinkPlacement.DATADIR_WIKIPEDIA;
    }
    CumulativeScoreLinkPlacement obj = new CumulativeScoreLinkPlacement(dir);
    obj.placeLinks((int) 1e6, new PrintStream(dir + "/link_placement_results_COINS-LINK.tsv"));
  }

}
