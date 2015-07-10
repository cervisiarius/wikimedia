package org.wikimedia.west1.traces.linkplacement.sourcebased;

public class LinkCandidate implements Comparable<LinkCandidate> {

  public String name;
  public float score;
  public float margGain;

  public LinkCandidate(String name, float score) {
    this.name = name;
    this.score = score;
    this.margGain = Float.NaN;
  }

  @Override
  public String toString() {
    return name;
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

}
