package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;

public class CumulativeScoreLinkPlacement extends LinkPlacement {

  public CumulativeScoreLinkPlacement(int numLinks, String datadir) throws IOException {
    super(numLinks, datadir);
    initMargGains();
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
    CumulativeScoreLinkPlacement placement = new CumulativeScoreLinkPlacement(100,
        LinkPlacement.DATADIR_SIMTK);
    System.err.println("DONE LOADING DATA");
    placement.placeLinks();
    System.err.format("Value: %s", placement.evaluate());
  }

}
