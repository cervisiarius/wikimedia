package org.wikimedia.west1.traces;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
// Although org.apache.hadoop.mapred.Mapper is deprecated, Hadoop streaming seems to require this
// one rather than the newer org.apache.hadoop.mapreduce.Mapper.
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.util.MarkupStripper;

public class LinkPositionExtractionMapper implements Mapper<Text, Text, Text, Text> {

  private MarkupStripper stripper = new MarkupStripper();
  private static Pattern REDIRECT_PATTERN = Pattern.compile("(?s).*<redirect title=\".*");
  private static Pattern TITLE_PATTERN = Pattern.compile("(?s).*?<title>(.*?)</title>.*");
  private static Pattern CONTENT_PATTERN = Pattern
      .compile("(?s).*?<text xml:space=\"preserve\">(.*)</text>.*");
  private static final String SIZE_TOKEN = "___SIZE___";

  public String format(String markup) {
    // First replace link markers so links don't get stripped.
    markup = markup.replace("[[", "##LEFT##").replace("]]", "##RIGHT##");
    // This is a hacky way of removing emphasis (as EmphasisResolver seems to be buggy; since it's
    // also used by stripAllButInternalLinksAndEmphasis, we need to remove emphasis manually first)
    markup = markup.replaceAll("'{6}", "'");
    markup = markup.replaceAll("'{5}", "");
    markup = markup.replaceAll("'{4}", "'");
    markup = markup.replaceAll("'{3}", "");
    markup = markup.replaceAll("'{2}", "");
    markup = stripper.stripAllButInternalLinksAndEmphasis(markup, null);
    markup = stripper.stripInternalLinks(markup, null);
    markup = stripper.stripExcessNewlines(markup);
    markup = StringEscapeUtils.unescapeHtml(markup);
    markup = markup.replace('\u2019', '\'');
    // Contract multiple whitespaces.
    markup = markup.replaceAll("\\s+", " ");
    return markup;
  }

  private static String normalize(String s) {
    s = s.replace(' ', '_');
    if (s.length() > 0) {
      s = s.substring(0, 1).toUpperCase() + s.substring(1);
    }
    return s;
  }

  private Map<String, List<Integer>> locateLinks(String s) {
    Map<String, List<Integer>> result = new HashMap<String, List<Integer>>();
    int i = 0;
    int size = 0;
    while (i < s.length()) {
      if (s.startsWith("##LEFT##", i)) {
        int linkEnd = s.indexOf("##RIGHT##", i) + 9;
        String link = s.substring(i + 8, linkEnd - 9);
        String target, anchor;
        if (link.contains("|")) {
          target = normalize(link.substring(0, link.indexOf('|')));
          anchor = link.substring(link.indexOf('|') + 1);
        } else {
          target = normalize(link);
          anchor = link;
        }
        int hashIdx = target.indexOf('#');
        if (hashIdx >= 0) {
          target = target.substring(0, hashIdx);
        }
        List<Integer> pos = result.get(target);
        if (pos == null) {
          pos = new ArrayList<Integer>();
          result.put(target, pos);
        }
        pos.add(i + 8);
        i = linkEnd;
        size += anchor.length();
      } else {
        ++i;
        ++size;
      }
    }
    result.put(SIZE_TOKEN, Arrays.asList(size));
    return result;
  }

  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    String xml = key.toString();
    String title, content;
    if (REDIRECT_PATTERN.matcher(xml).matches()) {
      return;
    }
    Matcher m = TITLE_PATTERN.matcher(xml);
    if (m.matches()) {
      title = normalize(m.group(1));
      // This additional filter is yet untested.
      if (title
          .matches("(?i)(Image|Media|Special|Talk|User|Wikipedia|File|MediaWiki|Template|Help|Book|Draft|Education_Program|TimedText|Module|Wikt)(_talk)?:.*")) {
        return;
      }
    } else {
      return;
    }
    m = CONTENT_PATTERN.matcher(xml);
    if (m.matches()) {
      content = m.group(1);
      content = format(content);
      Map<String, List<Integer>> linkLocs = locateLinks(content);
      int size = linkLocs.get(SIZE_TOKEN).get(0);
      for (String target : linkLocs.keySet()) {
        if (!target.equals(SIZE_TOKEN)) {
          List<Integer> pos = linkLocs.get(target);
          Collections.sort(pos);
          StringBuffer posString = new StringBuffer();
          String sep = "";
          for (int p : pos) {
            posString.append(sep).append(p);
            sep = ",";
          }
          output.collect(new Text(title), new Text(String.format("%s\t%s\t%s\n", target, size, posString)));
        }
      }
    } else {
      return;
    }
  }

  @Override
  public void configure(JobConf arg0) {
  }

  @Override
  public void close() throws IOException {
  }

  // Just for testing
  public static void main(String[] args) throws Exception {
    LinkPositionExtractionMapper obj = new LinkPositionExtractionMapper();
    obj.configure(null);
    String text = new Scanner(new File("/tmp/w.txt")).useDelimiter("\\Z").next();
    obj.map(new Text(text), new Text(""), null, null);
  }
}
