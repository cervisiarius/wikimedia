package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ChainModelLinkPlacement extends LinkPlacement {

  public ChainModelLinkPlacement(String datadir) throws IOException {
    super(datadir);
  }

  public double evaluate() {
    Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
    double value = 0;
    for (String src : bySource.keySet()) {
      double srcNewLinkScoreSum = 0;
      for (LinkCandidate cand : bySource.get(src)) {
        srcNewLinkScoreSum += cand.score;
      }
      // System.out.format("%s = %s * %s / %s\n", srcClickCounts.get(src) * srcNewLinkScoreSum
      // / (srcNewLinkScoreSum + srcExistingLinkScoreSum.get(src)), srcClickCounts.get(src),
      // srcNewLinkScoreSum, srcNewLinkScoreSum + srcExistingLinkScoreSum.get(src));
      value += srcClickCounts.get(src) * srcNewLinkScoreSum
          / (srcNewLinkScoreSum + srcExistingLinkScoreSum.get(src));
    }
    return value;
  }

  @Override
  protected void initMargGains() {
    int s = 0;
    for (String src : srcToCands.keySet()) {
      if (++s % 1000 == 0)
        System.err.format("Source %d of %s\n", s, srcCounts.size());
      // System.out.format("========== %s\n", src);
      List<LinkCandidate> cands = srcToCands.get(src);
      // Sort all cands by score.
      Collections.sort(cands, new LinkCandidate.ScoreComparator());
      double unnormCumSum = 0;
      double prevNormCumSum = 0;
      for (LinkCandidate cand : cands) {
        unnormCumSum += cand.score;
        double normCumsum = srcClickCounts.get(src) * unnormCumSum
            / (unnormCumSum + srcExistingLinkScoreSum.get(src));
        cand.margGain = normCumsum - prevNormCumSum;
        priorityQueue.offer(cand);
        prevNormCumSum = normCumsum;
      }
    }
  }

  @Override
  protected double computeScore(Map<String, Double> values) {
    return values.get("pst_indirect");
  }

  public static void main(String[] args) throws Exception {
    String dir;
    try {
      dir = args[0];
    } catch (ArrayIndexOutOfBoundsException e) {
      dir = LinkPlacement.DATADIR_WIKIPEDIA;
    }
    ChainModelLinkPlacement obj = new ChainModelLinkPlacement(dir);
    //obj.placeLinks((int) 1e3, System.out);
    obj.placeLinks((int) 1e6, new PrintStream(dir + "/link_placement_results_DICE.tsv"));
  }

}
