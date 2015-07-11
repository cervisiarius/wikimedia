package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.util.Comparator;

public class LinkCandidate implements Comparable<LinkCandidate> {

  public String src;
  public String tgt;
  public double score;
  public double margGain;
  private String name;

  public LinkCandidate(String src, String tgt, double score) {
    this.src = src;
    this.tgt = tgt;
    this.name = src + " > " + tgt;
    this.score = score;
    this.margGain = Double.NaN;
  }

  @Override
  public String toString() {
    return name + ":" + score;
  }

  @Override
  public boolean equals(Object other) {
    return name.equals(((LinkCandidate) other).name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  // Comparison is done based on the marginal gains; elements with larger marginal gains come
  // earlier in the ordering.
  public int compareTo(LinkCandidate other) {
    if (margGain < other.margGain)
      return 1;
    else if (margGain > other.margGain)
      return -1;
    else
      return 0;
  }
  
  public static class ScoreComparator implements Comparator<LinkCandidate> {
    @Override
    public int compare(LinkCandidate c1, LinkCandidate c2) {
      if (c1.score < c2.score)
        return 1;
      else if (c1.score > c2.score)
        return -1;
      else
        return 0;
    }
  }

}
