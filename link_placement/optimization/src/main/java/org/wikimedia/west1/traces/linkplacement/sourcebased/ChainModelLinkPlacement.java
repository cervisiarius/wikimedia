package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChainModelLinkPlacement extends LinkPlacement {

  public ChainModelLinkPlacement(String datadir, ScoreType scoreType) throws IOException {
    super(datadir, scoreType);
  }
  
  public double evaluate(Set<LinkCandidate> solution) {
    Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
    double value = 0;
    for (String src : bySource.keySet()) {
      double srcValue = 0;
      for (LinkCandidate cand : bySource.get(src)) {
        srcValue += cand.score;
      }
      // System.out.format("%s = %s * %s / %s\n", srcCounts.get(src) * srcValue
      // / (srcValue + srcExistingLinkScoreSum.get(src)), srcCounts.get(src), srcValue, srcValue
      // + srcExistingLinkScoreSum.get(src));
      srcValue = srcCounts.get(src) * srcValue / (srcValue + srcExistingLinkScoreSum.get(src));
      value += srcValue;
    }
    return value;
  }

  public static void main(String[] args) throws IOException {
    ChainModelLinkPlacement obj = new ChainModelLinkPlacement(LinkPlacement.DATADIR_WIKIPEDIA,
        ScoreType.P_ST);
    obj.placeLinks(100, false);
    System.out.println(obj.evaluate());
  }

}
