package org.wikimedia.west1.traces.linkplacement;

public class LinkCandidate implements Comparable<LinkCandidate> {
  
  public String name;
  public float score;
  public float margGain;
  public boolean polluted;
  
  public LinkCandidate(String name) {
    this.name = name;
    this.score = Float.NaN;
    this.margGain = Float.NaN;
    this.polluted = false;
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

  // Comparison is done based on the marginal gains.
  public int compareTo(LinkCandidate other) {
    return (int) (margGain - other.margGain);
  }

}
