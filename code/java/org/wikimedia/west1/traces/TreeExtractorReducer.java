package org.wikimedia.west1.traces;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONException;
import org.json.JSONObject;
import org.wikimedia.west1.traces.TreeExtractor.Pageview;

public class TreeExtractorReducer implements Reducer<Text, Text, Text, Text> {

  private TreeExtractor treeExtractor = new TreeExtractor();
  // Having 3600 pageviews in a day would mean one every 24 seconds, a lot...
  private static final int MAX_NUM_PAGEVIEWS = 3600;
  // If we see this much time between pageviews, we start a new session.
  private static final long INTER_SESSION_TIME = 3600 * 1000;

  @Override
  public void configure(JobConf conf) {
  }

  @Override
  public void close() throws IOException {
  }

  private List<Pageview> sequenceToTrees(List<Pageview> pageviews) throws JSONException,
      ParseException {
    // Sort all pageviews by time.
    Collections.sort(pageviews, new Comparator<Pageview>() {
      @Override
      public int compare(Pageview pv1, Pageview pv2) {
        // TODO: Order?
        return (int) (pv1.time - pv2.time);
      }
    });

    // Iterate over all pageviews in temporal order and split into sessions.
    List<Pageview> roots = new ArrayList<Pageview>();
    List<Pageview> session = new ArrayList<Pageview>();
    long prevTime = 0;
    for (Pageview pv : pageviews) {
      long curTime = pv.time;
      if (curTime - prevTime > INTER_SESSION_TIME) {
        roots.addAll(treeExtractor.getMinimumSpanningForest(session));
        session.clear();
      }
      prevTime = curTime;
    }

    return roots;
  }

  @Override
  public void reduce(Text uidAndDay, Iterator<Text> pageviewIterator,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    try {
      List<Pageview> pageviews = new ArrayList<Pageview>();
      int n = 0;

      // Collect all pageviews for this user on this day.
      while (pageviewIterator.hasNext()) {
        // If there are too many pageview events, output nothing.
        if (++n > MAX_NUM_PAGEVIEWS) {
          return;
        } else {
          pageviews.add(new Pageview(new JSONObject(pageviewIterator.next().toString())));
        }
      }
      // Extract trees and output them.
      List<Pageview> roots = sequenceToTrees(pageviews);
      for (Pageview root : roots) {
        out.collect(new Text(), new Text(roots.toString()));
      }
    } catch (Exception e) {
      System.out.format("%s\n", e.getMessage());
    }
  }

  private void test() throws Exception {
    List<Pageview> session = new ArrayList<Pageview>();
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:10\",\"url\":\"a\",\"referer\":\"-\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:15\",\"url\":\"c\",\"referer\":\"a\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:20\",\"url\":\"b\",\"referer\":\"a\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:25\",\"url\":\"a\",\"referer\":\"b\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:30\",\"url\":\"b\",\"referer\":\"a\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:40\",\"url\":\"c\",\"referer\":\"b\"}")));
    session.add(new Pageview(new JSONObject("{\"dt\":\"2014-12-04T01:00:50\",\"url\":\"d\",\"referer\":\"c\"}")));

    System.out.println(sequenceToTrees(session));
  }

  public static void main(String[] args) throws Exception {
    //String pvString = "{\"hostname\":\"cp1066.eqiad.wmnet\",\"sequence\":1470486742,\"dt\":\"2014-12-04T01:00:00\",\"time_firstbyte\":0.000128984,\"ip\":\"0.0.0.0\",\"cache_status\":\"hit\",\"http_status\":\"200\",\"response_size\":3185,\"http_method\":\"GET\",\"uri_host\":\"en.wikipedia.org\",\"uri_path\":\"/w/index.php\",\"uri_query\":\"?title=MediaWiki:Gadget-refToolbarBase.js&action=raw&ctype=text/javascript\",\"content_type\":\"text/javascript; charset=UTF-8\",\"referer\":\"http://es.wikipedia.org/wiki/Jos%C3%A9_Mar%C3%ADa_Yazpik\",\"x_forwarded_for\":\"-\",\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\",\"accept_language\":\"es-MX\",\"x_analytics\":\"php=hhvm\",\"range\":\"-\"}";
    //JSONObject pv = new JSONObject(pvString);
    //System.out.println(pv.getString("user_agent"));
    TreeExtractorReducer reducer = new TreeExtractorReducer();
    reducer.test();
  }

}
