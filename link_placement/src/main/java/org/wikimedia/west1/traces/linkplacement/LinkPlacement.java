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

public abstract class LinkPlacement {

  protected int numLinks;
  protected Set<LinkCandidate> solution;
  protected PriorityQueue<LinkCandidate> priorityQueue;

  protected Map<LinkCandidate, int[]> candsToTrees;
  protected Map<Integer, LinkCandidate[]> treesToCands;

  public final static String DATADIR_WIKIPEDIA = System.getenv("HOME") + "/wikimedia/trunk/data/";
  public final static String DATADIR_SIMTK = System.getenv("HOME")
      + "/wikimedia/trunk/data/simtk/pair_counts/";

  private String datadir;

  public LinkPlacement(int numLinks, String datadir) throws IOException {
    this.datadir = datadir;
    this.solution = new HashSet<LinkCandidate>();
    this.numLinks = numLinks;
    loadTreeData();
  }

  public abstract void placeLinks();

  protected void initMargGains() {
    for (LinkCandidate cand : candsToTrees.keySet()) {
      cand.margGain = margGain(cand);
    }
    priorityQueue = new PriorityQueue<LinkCandidate>();
    for (LinkCandidate cand : candsToTrees.keySet()) {
      priorityQueue.offer(cand);
    }
  }

  private void loadTreeData() throws IOException {
    Map<String, Float> candScores = loadCandScores();
    candsToTrees = new HashMap<LinkCandidate, int[]>();
    Map<Integer, List<LinkCandidate>> treesToCandLists = new HashMap<Integer, List<LinkCandidate>>();
    Map<String, Integer> treeIndex = new HashMap<String, Integer>();
    FileInputStream fis = new FileInputStream(datadir + "link_candidates_tree_ids.tsv.gz");
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
    FileInputStream fis = new FileInputStream(datadir
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

  public float evaluate() {
    Set<Integer> trees = new HashSet<Integer>();
    for (LinkCandidate cand : solution) {
      for (int tree : candsToTrees.get(cand)) {
        trees.add(tree);
      }
    }
    float value = 0;
    for (int tree : trees) {
      float p_none = 1;
      for (LinkCandidate cand : treesToCands.get(tree)) {
        if (solution.contains(cand)) {
          p_none *= (1 - cand.score);
        }
      }
      value += (1 - p_none);
    }
    return value;
  }

  protected float margGain(LinkCandidate cand) {
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

}
