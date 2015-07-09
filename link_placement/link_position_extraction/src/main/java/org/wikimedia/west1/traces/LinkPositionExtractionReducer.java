package org.wikimedia.west1.traces;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LinkPositionExtractionReducer implements Reducer<Text, Text, Text, Text> {

  // The redirects; they're read from file when this Mapper instance is created.
  private Map<String, String> redirects = new HashMap<String, String>();

  private void readRedirectsFromInputStream(InputStream is) {
    Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 2);
      String src = tokens[0];
      String tgt = tokens[1];
      redirects.put(src, tgt);
    }
    sc.close();
  }

  private InputStream getHdfsInputStream() throws IOException {
    Path path = new Path("hdfs:///user/west1/wikipedia_redirects/enwiki_20141008_redirects.tsv");
    FileSystem fs = FileSystem.get(new Configuration());
    return fs.open(path);
  }

  @Override
  public void configure(JobConf conf) {
    try {
      InputStream is;
      if (conf == null) {
        is = new GZIPInputStream(
            new FileInputStream(
                "/afs/cs.stanford.edu/u/west1/wikimedia/trunk/data/redirects/enwiki_20141008_redirects.tsv.gz"));
      } else {
        is = getHdfsInputStream();
      }
      readRedirectsFromInputStream(is);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void reduce(Text key, Iterator<Text> it, OutputCollector<Text, Text> out, Reporter reporter)
      throws IOException {
    Map<String, StringBuffer> linkPosMap = new HashMap<String, StringBuffer>();
    String size = null;
    while (it.hasNext()) {
      String[] tokens = it.next().toString().split("\t", 3);
      String target = tokens[0];
      if (size == null) {
        size = tokens[1];
      }
      String posString = tokens[2];
      String resolved;
      if ((resolved = redirects.get(target)) != null) {
        target = resolved;
      }
      StringBuffer pos = linkPosMap.get(target);
      if (pos == null) {
        pos = new StringBuffer(posString);
        linkPosMap.put(target, pos);
      } else {
        pos.append(',').append(posString);
      }
    }
    for (String target : linkPosMap.keySet()) {
      out.collect(key, new Text(String.format("%s\t%s\t%s", target, size, linkPosMap.get(target))));
      // System.out.println(String.format("%s\t%s\t%s", target, size, linkPosMap.get(target)));
    }
  }

  // Just for testing
  public static void main(String[] args) throws Exception {
    LinkPositionExtractionReducer obj = new LinkPositionExtractionReducer();
    obj.configure(null);
    Iterator<Text> it = Arrays.asList(
        new Text("Pierre-Joseph_Proudhon\t4445656\t11642,25139,27862,74921,129304"),
        new Text("AccessibleComputing\t4343456\t4"),
        new Text("Computer_accessibility\t4343456\t7654")).iterator();
    obj.reduce(new Text("Anarchism"), it, null, null);
  }
}
