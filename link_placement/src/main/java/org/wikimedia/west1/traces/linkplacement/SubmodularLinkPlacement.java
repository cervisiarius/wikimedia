package org.wikimedia.west1.traces.linkplacement;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class SubmodularLinkPlacement {

  private int numLinks;
  private Map<LinkCandidate, int[]> candsToTrees;
  private Map<Integer, LinkCandidate[]> treesToCands;
  private Set<LinkCandidate> solution;
  private PriorityQueue<LinkCandidate> priorityQueue;

  private static final String DATADIR = System.getenv("HOME") + "/wikimedia/trunk/data/";

  public SubmodularLinkPlacement(int numLinks) throws IOException {
    this.solution = new HashSet<LinkCandidate>();
    this.numLinks = numLinks;
    loadTreeData();
    initMargGains();
  }

  private void loadTreeData() throws IOException {
    Map<String, Float> candScores = loadCandScores();
    candsToTrees = new HashMap<LinkCandidate, int[]>();
    Map<Integer, List<LinkCandidate>> treesToCandLists = new HashMap<Integer, List<LinkCandidate>>();
    Map<String, Integer> treeIndex = new HashMap<String, Integer>();
    FileInputStream fis = new FileInputStream(DATADIR + "link_candidates_tree_ids.tsv.gz");
    Scanner sc = new Scanner(new GZIPInputStream(fis), "UTF-8").useDelimiter("\n");
    int lastInt = 0;
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 3);
      String candName = String.format("%s|%s", tokens[0], tokens[1]);
      // Only consider candidates with a defined score.
      if (candScores.containsKey(candName)) {
        LinkCandidate cand = new LinkCandidate(candName, candScores.get(candName));
        String[] treeIds = tokens[2].split(",");
        int[] treeInts = new int[treeIds.length];
        for (int i = 0; i < treeIds.length; ++i) {
          Integer treeInt = treeIndex.get(treeIds[i]);
          if (treeInt == null) {
            treeInt = lastInt;
            treeIndex.put(treeIds[i], lastInt);
            ++lastInt;
          }
          treeInts[i] = treeInt;
          List<LinkCandidate> candList = treesToCandLists.get(treeInt);
          if (candList == null) {
            candList = new ArrayList<LinkCandidate>();
            treesToCandLists.put(treeInt, candList);
          }
          candList.add(cand);
        }
        candsToTrees.put(cand, treeInts);
      }
    }
    fis.close();
    sc.close();
    // Store the tree-to-cand map more efficiently with arrays rather than lists as values.
    treesToCands = new HashMap<Integer, LinkCandidate[]>();
    for (int treeId : treesToCandLists.keySet()) {
      List<LinkCandidate> list = treesToCandLists.get(treeId);
      LinkCandidate[] array = new LinkCandidate[list.size()];
      treesToCands.put(treeId, list.toArray(array));
    }
  }

  private Map<String, Float> loadCandScores() throws IOException {
    Map<String, Float> result = new HashMap<String, Float>();
    FileInputStream fis = new FileInputStream(DATADIR
        + "link_candidates_scores_GROUND-TRUTH.tsv.gz");
    Scanner sc = new Scanner(new GZIPInputStream(fis), "UTF-8").useDelimiter("\n");
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 6);
      // TODO: maybe impose a threshold on candidate frequency?
      result.put(String.format("%s|%s", tokens[0], tokens[1]), Float.parseFloat(tokens[2]));
    }
    fis.close();
    sc.close();
    return result;
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
      // If the solution is empty, then the condition in the loop will always be false, so we can
      // skip the for loop to begin with.
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
    // Pick the numLinks optimal candidates greedily.
    for (int i = 0; i < numLinks; ++i) {
      // Get candidates from the priority queue until we find an uncorrupted one.
      while (true) {
        // Pick the candidate with the largest marginal gain (which may be corrupted).
        LinkCandidate cand = priorityQueue.poll();
        // If cand's marginal gain is corrupted, recompute it, mark cand as uncorrupted, and
        // reinsert it into the priority queue.
        if (cand.corrupted) {
          float oldMargGain = cand.margGain;
          cand.margGain = margGain(cand);
          cand.corrupted = false;
          priorityQueue.offer(cand);
          Set<LinkCandidate> neighborsInSolution = new HashSet<LinkCandidate>();
          for (int tree : candsToTrees.get(cand)) {
            for (LinkCandidate neighbor : treesToCands.get(tree)) {
              if (solution.contains(neighbor)) {
                neighborsInSolution.add(neighbor);
              }
            }
          }
          System.out.format("### CORRPUPT: %s, score: %s, old gain: %s, new gain: %s, neigh: %s\n",
              cand.name, cand.score, oldMargGain, cand.margGain, neighborsInSolution.toString());
        }
        // If cand's marginal gain is uncorrupted, cand is the optimal next greedy choice. Mark all
        // other candidates that appear in trees together with cand as corrupted, add cand to the
        // solution, and break from the while loop.
        else {
          for (int tree : candsToTrees.get(cand)) {
            for (LinkCandidate neighbor : treesToCands.get(tree)) {
              neighbor.corrupted = true;
            }
          }
          System.out.format("(%d) %s, score: %s, gain: %s, #paths: %d\n", i, cand.name, cand.score,
              cand.margGain, candsToTrees.get(cand).length);
          solution.add(cand);
          break;
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    SubmodularLinkPlacement placement = new SubmodularLinkPlacement(100);
    System.err.println("DONE LOADING DATA");
    placement.placeLinks();
    // System.out.println(placement.candsToTrees.get(new
    // LinkCandidate("Port_Klang_Line|KLIA_Transit")));
  }

}
