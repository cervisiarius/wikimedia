package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeModelLinkPlacement extends LinkPlacement {

  public TreeModelLinkPlacement(String datadir, ScoreType scoreType) throws IOException {
    super(datadir, scoreType);
  }

  public double evaluate(Set<LinkCandidate> solution) {
    Map<String, List<LinkCandidate>> bySource = splitSolutionBySource(solution);
    double value = 0;
    for (String src : bySource.keySet()) {
      double probNone = 1;
      for (LinkCandidate cand : bySource.get(src)) {
        probNone *= 1 - cand.score;
      }
      // System.out.format("%s = %s * (1 - %s)\n", srcCounts.get(src) * (1 - probNone),
      // srcCounts.get(src), probNone);
      value += srcCounts.get(src) * (1 - probNone);
    }
    return value;
  }

  public static void main(String[] args) throws IOException {
    TreeModelLinkPlacement obj = new TreeModelLinkPlacement(LinkPlacement.DATADIR_WIKIPEDIA,
        ScoreType.P_ST);
    obj.placeLinks(100, false);
  }

}
