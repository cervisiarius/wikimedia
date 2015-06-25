package org.wikimedia.west1.traces.linkplacement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public class SubmodularLinkPlacement {

  private int numLinks;
  private Map<LinkCandidate, int[]> candsToTrees;
  private Map<Integer, LinkCandidate[]> treesToCands;
  private Set<LinkCandidate> solution;
  private PriorityQueue<LinkCandidate> priorityQueue;

  public SubmodularLinkPlacement(int numLinks) {
    this.numLinks = numLinks;
    loadTreeData();
    loadCandScores();
    initMargGains();
    solution = new HashSet<LinkCandidate>();
  }

  private void loadTreeData() {
    candsToTrees = new HashMap<LinkCandidate, int[]>();
    treesToCands = new HashMap<Integer, LinkCandidate[]>();
    // TODO
  }

  private void loadCandScores() {
    // TODO
  }

  private void initMargGains() {
    for (LinkCandidate cand : candsToTrees.keySet()) {
      cand.margGain = margGain(cand);
    }
    priorityQueue = new PriorityQueue<LinkCandidate>();
    for (LinkCandidate cand : candsToTrees.keySet()) {
      priorityQueue.offer(cand);
    }
  }

  private float margGain(LinkCandidate cand) {
    float mg = 0;
    for (int tree : candsToTrees.get(cand)) {
      float inc = cand.score;
      // This needs not be done in the init phase.
      if (!solution.isEmpty()) {
        for (LinkCandidate neighbor : treesToCands.get(tree)) {
          if (solution.contains(neighbor)) {
            inc *= (1 - neighbor.score);
          }
        }
      }
      mg += inc;
    }
    return mg;
  }

  private void placeLinks() {
    for (int i = 0; i < numLinks; ++i) {
      while (true) {
        LinkCandidate cand = priorityQueue.poll();
        if (cand == null) {
          throw new IllegalStateException("Priority queue is empty");
        } else if (cand.polluted) {
          cand.margGain = margGain(cand);
          priorityQueue.offer(cand);
          cand.polluted = false;
        } else {
          for (int tree : candsToTrees.get(cand)) {
            for (LinkCandidate neighbor : treesToCands.get(tree)) {
              neighbor.polluted = true;
            }
          }
          solution.add(cand);
          break;
        }
      }
    }
  }

  public static void main(String[] args) {
    SubmodularLinkPlacement placement = new SubmodularLinkPlacement(100);
    placement.placeLinks();
    System.out.println(placement.solution);
  }

}
