package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;

public abstract class LinkPlacement {

  protected Set<LinkCandidate> solution;
  protected PriorityQueue<LinkCandidate> priorityQueue;

  protected Map<String, List<LinkCandidate>> srcToCands = new HashMap<String, List<LinkCandidate>>();
  protected Map<String, Double> srcCounts = new HashMap<String, Double>();
  protected Map<String, Double> srcClickCounts = new HashMap<String, Double>();
  protected Map<String, Double> srcExistingLinkScoreSum = new HashMap<String, Double>();

  public final static String DATADIR_WIKIPEDIA = System.getenv("HOME")
      + "/wikimedia/trunk/data/link_placement/";
  public final static String DATADIR_SIMTK = System.getenv("HOME")
      + "/wikimedia/trunk/data/simtk/pair_counts/";

  public LinkPlacement(String datadir) throws IOException {
    this.solution = new HashSet<LinkCandidate>();
    this.priorityQueue = new PriorityQueue<LinkCandidate>();
    long before = System.currentTimeMillis();
    System.err.println("Reading data... ");
    loadDataFromTextFile(datadir, false);
    System.err.format("Done reading data (%d sec)\n", (System.currentTimeMillis() - before) / 1000);
  }

  private static double transformCount(double x) {
    return Math.log(x);
  }

  @SuppressWarnings("unused")
  private void loadDataFromSerializedFile(String datadir) throws IOException,
      ClassNotFoundException {
    InputData data = InputData.deserialize(datadir + "masterfile.ser");
    for (String candName : data.map.keySet()) {
      String[] tokens = candName.split(" > ", 2);
      String src = tokens[0];
      String tgt = tokens[1];
      Map<String, Double> values = data.map.get(candName);
      List<LinkCandidate> candList = srcToCands.get(src);
      if (candList == null) {
        candList = new ArrayList<LinkCandidate>();
        srcToCands.put(src, candList);
      }
      srcCounts.put(src, transformCount(values.get("source_count_before")));
      srcClickCounts.put(src, transformCount(values.get("source_transition_count_before")));
      srcExistingLinkScoreSum.put(src,
          values.get("source_transition_count_before") / values.get("source_count_before"));
      LinkCandidate cand = new LinkCandidate(src, tgt, computeScore(values));
      candList.add(cand);
    }
  }

  private void loadDataFromTextFile(String datadir, boolean serialize) throws IOException {
    InputData data = new InputData();
    FileInputStream fis = new FileInputStream(datadir + "/masterfile.tsv");
    Scanner sc = new Scanner(fis, "UTF-8").useDelimiter("\n");
    // Read the header.
    String[] colnames = sc.next().split("\t", 22);
    int i = 0;
    while (sc.hasNext()) {
      if (++i % 1e6 == 0)
        System.err.println(i);
      // if (i > 1e6) break;
      String[] tokens = sc.next().split("\t", colnames.length);
      String src = tokens[0];
      String tgt = tokens[1];
      Map<String, Double> values = new HashMap<String, Double>();
      try {
        for (int c = 2; c < colnames.length; ++c) {
          values.put(colnames[c], Double.parseDouble(tokens[c]));
        }
        List<LinkCandidate> candList = srcToCands.get(src);
        if (candList == null) {
          candList = new ArrayList<LinkCandidate>();
          srcToCands.put(src, candList);
        }
        srcCounts.put(src, transformCount(values.get("source_count_before")));
        srcClickCounts.put(src, transformCount(values.get("source_transition_count_before")));
        srcExistingLinkScoreSum.put(src,
            values.get("source_transition_count_before") / values.get("source_count_before"));
        LinkCandidate cand = new LinkCandidate(src, tgt, computeScore(values));
        candList.add(cand);
        if (serialize) {
          data.map.put(cand.getName(), values);
        }
      } catch (NumberFormatException e) {
        // continue;
        throw e;
      }
    }
    if (serialize) {
      System.err.println("Serializing...");
      data.serialize(datadir + "/masterfile.ser");
    }
    fis.close();
    sc.close();
  }

  protected Map<String, List<LinkCandidate>> splitSolutionBySource(Set<LinkCandidate> solution) {
    Map<String, List<LinkCandidate>> bySource = new HashMap<String, List<LinkCandidate>>();
    for (LinkCandidate cand : solution) {
      List<LinkCandidate> candsForSrc = bySource.get(cand.src);
      if (candsForSrc == null) {
        candsForSrc = new ArrayList<LinkCandidate>();
        bySource.put(cand.src, candsForSrc);
      }
      candsForSrc.add(cand);
    }
    return bySource;
  }

  protected abstract double computeScore(Map<String, Double> values);

  public abstract double evaluate();

  protected abstract void initMargGains();

  public void placeLinks(int numLinks, PrintStream out) {
    System.err.println("Computing marginal gains per source... ");
    initMargGains();
    System.err.println("Optimizing greedily");
    if (out != null) {
      out.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", "pos", "src", "tgt", "marg_gain",
          "pst_hat", "source_count_before", "source_transition_count_before",
          "avg_nonleaf_source_deg_before", "chain_marg_gain", "tree_marg_gain", "coins_marg_gain");
    }
    // These keep track of the values for each source separately (sources are independent of each
    // other).
    Map<String, Double> unnormCumSums = new HashMap<String, Double>();
    Map<String, Double> probNones = new HashMap<String, Double>();
    Map<String, Double> prevChainValues = new HashMap<String, Double>();
    Map<String, Double> prevTreeValues = new HashMap<String, Double>();
    Map<String, Double> prevCoinsValues = new HashMap<String, Double>();
    // Pick the numLinks optimal candidates greedily w.r.t. marginal gains.
    for (int i = 0; i < numLinks; ++i) {
      // Pick the candidate with the largest marginal gain.
      LinkCandidate cand = priorityQueue.poll();
      // Compute the value under the chain objective.
      Double unnormCumSum = unnormCumSums.get(cand.src);
      unnormCumSum = (unnormCumSum == null) ? cand.score : unnormCumSum + cand.score;
      unnormCumSums.put(cand.src, unnormCumSum);
      double chainValue = srcClickCounts.get(cand.src) * unnormCumSum
          / (unnormCumSum + srcExistingLinkScoreSum.get(cand.src));
      Double prevChainValue = prevChainValues.get(cand.src);
      double chainMargGain = chainValue - (prevChainValue == null ? 0.0 : prevChainValue);
      prevChainValues.put(cand.src, chainValue);
      // Compute the value under the coins objective.
      double coinsValue = srcCounts.get(cand.src) * unnormCumSum;
      Double prevCoinValue = prevCoinsValues.get(cand.src);
      double coinsMargGain = coinsValue - (prevCoinValue == null ? 0.0 : prevCoinValue);
      prevCoinsValues.put(cand.src, coinsValue);
      // Compute the value under the tree objective.
      Double probNone = probNones.get(cand.src);
      probNone = (probNone == null) ? (1 - cand.score) : probNone * (1 - cand.score);
      probNones.put(cand.src, probNone);
      double treeValue = srcCounts.get(cand.src) * (1 - probNone);
      Double prevTreeValue = prevTreeValues.get(cand.src);
      double treeMargGain = treeValue - (prevTreeValue == null ? 0.0 : prevTreeValue);
      prevTreeValues.put(cand.src, treeValue);
      solution.add(cand);
      if (out != null) {
        out.format("%d\t%s\t%s\t%.5f\t%.5f\t%.5f\t%.5f\t%.5f\t%.5f\t%.5f\t%.5f\n", i, cand.src,
            cand.tgt, cand.margGain, cand.score, srcCounts.get(cand.src),
            srcClickCounts.get(cand.src), srcExistingLinkScoreSum.get(cand.src), chainMargGain,
            treeMargGain, coinsMargGain);
      }
    }
  }

}

// private void loadData() throws IOException {
// srcToCands = new HashMap<String, List<LinkCandidate>>();
// srcCounts = new HashMap<String, Integer>();
// srcClickCounts = new HashMap<String, Integer>();
// srcExistingLinkScoreSum = new HashMap<String, Double>();
// FileInputStream fis = new FileInputStream(datadir + "global_optimization_input.tsv.gz");
// Scanner sc = new Scanner(new GZIPInputStream(fis), "UTF-8").useDelimiter("\n");
// // Read the header.
// String[] colnames = sc.next().split("\t", 10);
// while (sc.hasNext()) {
// String[] tokens = sc.next().split("\t", colnames.length);
// String src = tokens[0];
// String tgt = tokens[1];
// int isTop7k = Integer.parseInt(tokens[2]);
// // For now ignore those that are not in the top 7k, since we don't have p_cum_existing yet for
// // them.
// if (isTop7k == 1) {
// int srcCount = Integer.parseInt(tokens[3]);
// try {
// double existingLinkScoreCount = Double.parseDouble(tokens[4]);
// srcCounts.put(src, srcCount);
// srcClickCounts.put(src, srcCount); ///////////// abuse srcCount here for testing.
// srcExistingLinkScoreSum.put(src, existingLinkScoreCount);
// for (int c = 4; c < colnames.length; ++c) {
// if (colnames[c].toLowerCase().equals("p_st")) {
// List<LinkCandidate> candList = srcToCands.get(src);
// if (candList == null) {
// candList = new ArrayList<LinkCandidate>();
// srcToCands.put(src, candList);
// }
// candList.add(new LinkCandidate(src, tgt, Double.parseDouble(tokens[c])));
// }
// }
// } catch (NumberFormatException e) {
// continue;
// }
// }
// }
// fis.close();
// sc.close();
// }
