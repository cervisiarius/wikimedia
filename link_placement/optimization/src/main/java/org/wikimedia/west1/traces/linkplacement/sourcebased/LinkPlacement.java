package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public abstract class LinkPlacement {

  protected Set<LinkCandidate> solution;
  protected PriorityQueue<LinkCandidate> priorityQueue;

  protected Map<String, List<LinkCandidate>> srcToCands;
  protected Map<String, Integer> srcCounts;
  protected Map<String, Double> srcExistingLinkScoreSum;

  public final static String DATADIR_WIKIPEDIA = System.getenv("HOME")
      + "/wikimedia/trunk/data/link_placement/";
  public final static String DATADIR_SIMTK = System.getenv("HOME")
      + "/wikimedia/trunk/data/simtk/pair_counts/";

  private String datadir;
  private ScoreType scoreType;

  public enum ScoreType {
    P_ST, P_INDIR, P_SEARCH, P_NULL, P_MEAN_ALL
  }

  public LinkPlacement(String datadir, ScoreType scoreType) throws IOException {
    this.datadir = datadir;
    this.scoreType = scoreType;
    this.solution = new HashSet<LinkCandidate>();
    this.priorityQueue = new PriorityQueue<LinkCandidate>();
    loadData();
  }

  private void loadData() throws IOException {
    srcToCands = new HashMap<String, List<LinkCandidate>>();
    srcCounts = new HashMap<String, Integer>();
    srcExistingLinkScoreSum = new HashMap<String, Double>();
    FileInputStream fis = new FileInputStream(datadir + "global_optimization_input.tsv.gz");
    Scanner sc = new Scanner(new GZIPInputStream(fis), "UTF-8").useDelimiter("\n");
    // Read the header.
    String[] colnames = sc.next().split("\t", 10);
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", colnames.length);
      String src = tokens[0];
      String tgt = tokens[1];
      int isTop7k = Integer.parseInt(tokens[2]);
      // For now ignore those that are not in the top 7k, since we don't have p_cum_existing yet for
      // them.
      if (isTop7k == 1) {
        int srcCount = Integer.parseInt(tokens[3]);
        try {
          double existingLinkScoreCount = Double.parseDouble(tokens[4]);
          srcCounts.put(src, srcCount);
          srcExistingLinkScoreSum.put(src, existingLinkScoreCount);
          for (int c = 4; c < colnames.length; ++c) {
            if (colnames[c].toLowerCase().equals(scoreType.toString().toLowerCase())) {
              List<LinkCandidate> candList = srcToCands.get(src);
              if (candList == null) {
                candList = new ArrayList<LinkCandidate>();
                srcToCands.put(src, candList);
              }
              candList.add(new LinkCandidate(src, tgt, Double.parseDouble(tokens[c])));
            }
          }
        } catch (NumberFormatException e) {
          continue;
        }
      }
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

  public double evaluate() {
    return evaluate(solution);
  }

  public abstract double evaluate(Set<LinkCandidate> solution);

  public void placeLinks(int numLinks, boolean quiet) {
    for (String src : srcToCands.keySet()) {
      //System.out.format("========== %s\n", src);
      List<LinkCandidate> cands = srcToCands.get(src);
      // Sort all cands by score.
      Collections.sort(cands, new LinkCandidate.ScoreComparator());
      Set<LinkCandidate> localSolution = new HashSet<LinkCandidate>();
      double value = 0;
      for (LinkCandidate cand : cands) {
        localSolution.add(cand);
        double newValue = evaluate(localSolution);
        cand.margGain = newValue - value;
        priorityQueue.offer(cand);
        value = newValue;
      }
    }
    // Pick the numLinks optimal candidates greedily w.r.t. marginal gains.
    for (int i = 0; i < numLinks; ++i) {
      // Pick the candidate with the largest marginal gain.
      LinkCandidate cand = priorityQueue.poll();
      solution.add(cand);
      if (!quiet) {
        System.out.format("(%d) %s, gain: %.1f, n_src: %s, p_cum_ex: %.5f\n", i, cand,
            cand.margGain, srcCounts.get(cand.src), srcExistingLinkScoreSum.get(cand.src));
      }
    }
  }

}
