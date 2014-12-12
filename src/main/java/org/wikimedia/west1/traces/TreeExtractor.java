package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractor {

  public static class Pageview {
    public JSONObject json;
    public long time;

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public Pageview(JSONObject json) throws JSONException, ParseException {
      this.json = json;
      this.time = DATE_FORMAT.parse(json.getString("dt")).getTime();
    }

    public String toString() {
      return json.toString();
    }

    public String toString(int indentFactor) {
      try {
        return json.toString(indentFactor);
      } catch (JSONException e) {
        System.err.println(json);
        return "[JSONException]";
      }
    }
  }

  // Input: the list of session pageviews in temporal order.
  // Output: the list of tree roots.
  public List<Pageview> getMinimumSpanningForest(List<Pageview> session) throws JSONException {
    List<Pageview> roots = new ArrayList<Pageview>();
    Map<String, Pageview> urlToLastPageview = new HashMap<String, Pageview>();
    // Iterate over all pageviews in temporal order.
    for (Pageview pv : session) {
      Pageview parent = urlToLastPageview.get(pv.json.getString("referer"));
      if (parent == null) {
        roots.add(pv);
      } else {
        parent.json.append("z_children", pv.json);
      }
      // Remember this pageview as the last pageview for its URL.
      urlToLastPageview.put(pv.json.getString("url"), pv);
    }
    return roots;
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

    List<Pageview> roots = getMinimumSpanningForest(session);
    for (Pageview root : roots) {
      System.out.println(root.toString(2));
    }
  }

  public static void main(String[] args) throws Exception {
    TreeExtractor ex = new TreeExtractor();
    ex.test();
  }

}
