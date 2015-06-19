package org.wikimedia.west1.traces;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractorReducer implements Reducer<Text, Text, Text, Text> {

  // JSON field names.
  private static final String JSON_TREE_ID = "id";
  private static final String JSON_CHILDREN = "children";
  private static final String JSON_PARENT_AMBIGUOUS = "parent_ambiguous";
  private static final String JSON_BAD_TREE = "bad_tree";
  private static final String JSON_DT = "dt";
  private static final String JSON_UA = "user_agent";
  private static final String JSON_TITLE = "title";
  private static final String JSON_HTTP_STATUS = "http_status";
  private static final String JSON_REFERER = "referer";
  private static final String JSON_UNRESOLVED_TITLE = "unresolved_title";
  private static final String JSON_IS_SEARCH = "is_search";
  private static final String JSON_SEARCH_PARAMS = "search_params";
  // Job config parameters specifying which Wikipedia versions we're interested in, e.g.,
  // "(pt|es)\\.wikipedia\\.org".
  private static final String CONF_LANGUAGE_PATTERN = "org.wikimedia.west1.traces.languagePattern";
  // Job config parameters specifying if we want to keep trees in which at least one node has an
  // ambiguous parent (the algorithm will always pick the temporally closest one).
  private static final String CONF_KEEP_AMBIGUOUS_TREES = "org.wikimedia.west1.traces.keepAmbiguousTrees";
  // Job config parameters specifying if we want to keep trees whose root is from a Wikimedia site.
  private static final String CONF_KEEP_BAD_TREES = "org.wikimedia.west1.traces.keepBadTrees";
  // Job config parameters specifying if we want to keep trees consisting of a single event.
  private static final String CONF_KEEP_SINGLETON_TREES = "org.wikimedia.west1.traces.keepSingletonTrees";
  // Job config parameters specifying a string for salting UID hashes, so it's very hard to get the
  // hash value for a given UID.
  private static final String CONF_HASH_SALT = "org.wikimedia.west1.traces.hashSalt";
  // If a user has more than this many events, we ignore her.
  private static final String CONF_MAX_NUM_EVENTS = "org.wikimedia.west1.traces.maxNumEvents";
  // To avoid StackOverflowErrors, we set a maximum depth for the recursive method isGoodTree.
  private static final int MAX_RECURSION_DEPTH = 100;
  // A pattern matching Wikimedia host names.
  private static final Pattern WIKI_HOST_PATTERN = Pattern.compile("[a-z]+://[^/]*"
  // Adapted from
  // https://github.com/wikimedia/analytics-refinery-source/blob/master/refinery-core/src/...
  // .../main/java/org/wikimedia/analytics/refinery/core/Pageview.java.
  // + "wik(ibooks|idata|inews|imedia|ipedia|iquote|isource|tionary|iversity|ivoyage)\\.org.*";
      + "wiki(data|media|pedia)\\.org.*");

  // The fields you want to store for every event.
  private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(JSON_DT,
      JSON_TITLE, JSON_HTTP_STATUS,
      // The fields we added.
      JSON_UNRESOLVED_TITLE, JSON_CHILDREN, JSON_PARENT_AMBIGUOUS, JSON_BAD_TREE, JSON_IS_SEARCH,
      JSON_SEARCH_PARAMS));
  // The fields you want to store only for the root (because they're identical for all events in
  // the same tree).
  private static final Set<String> FIELDS_TO_KEEP_IN_ROOT = new HashSet<String>(Arrays.asList(
      JSON_UA, JSON_REFERER, JSON_TREE_ID));

  private static enum HADOOP_COUNTERS {
    // In order to have an idea what the big reasons are for dismissing trees. Note that these don't
    // add up to the number of trees we start with before filtering, since filtering fails fast (cf.
    // isGoodEvent).
    REDUCE_SKIPPED_SINGLETON, REDUCE_SKIPPED_HOUR_23, REDUCE_SKIPPED_WIKIMEDIA_REFERER_IN_ROOT, REDUCE_SKIPPED_AMBIGUOUS, REDUCE_TOO_MANY_EVENTS, REDUCE_STACKOVERFLOW,
    // OK_TREE and BAD_TREE add to the number of trees we started with before filtering.
    REDUCE_OK_TREE, REDUCE_BAD_TREE, REDUCE_EXCEPTION, REDUCE_REDIRECT_RESOLVED,
    // Timers etc.
    REDUCE_MSEC_EVENT_CONSTRUCTOR, REDUCE_MAX_MEMORY
  }

  private Pattern homePagePattern;
  private boolean keepAmbiguousTrees;
  private boolean keepBadTrees;
  private boolean keepSingletonTrees;
  private String hashSalt;
  private int maxNumEvents;

  // The redirects; they're read from file when this Mapper instance is created.
  private Map<String, Map<String, String>> redirects = new HashMap<String, Map<String, String>>();

  private void readRedirectsFromInputStream(String lang, InputStream is) {
    Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
    Map<String, String> redirectsForLang = new HashMap<String, String>();
    redirects.put(lang, redirectsForLang);
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 2);
      String src = tokens[0];
      String tgt = tokens[1];
      redirectsForLang.put(src, tgt);
    }
    sc.close();
  }

  @SuppressWarnings("unused")
  private InputStream getJarInputStream(String file) {
    return ClassLoader.getSystemResourceAsStream(file);
  }

  private InputStream getHdfsInputStream(String file) throws IOException {
    Path path = new Path("hdfs:///user/west1/redirects/" + file);
    FileSystem fs = FileSystem.get(new Configuration());
    return fs.open(path);
  }

  private void readAllRedirects(String[] languages) {
    for (String lang : languages) {
      String file = lang + "_redirects.tsv.gz";
      try {
        InputStream is = new GZIPInputStream(getHdfsInputStream(file));
        readRedirectsFromInputStream(lang, is);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // Tree ids consist of the day, a salted hash of the UID, and a sequential number (in order of
  // time), e.g., 5ca697716da3203201f56d09b41c954d_20150118_0004.
  private Text makeTreeId(String lang, String uid, int seqNum) {
    String uidHash = DigestUtils.md5Hex(uid + hashSalt);
    // seqNum is zero-padded to fixed length 5: we allow at most MAX_NUM_EVENTS = 100K events
    // per day, and in the worst case, each event is its own tree, so seqNum <= 99999.
    return new Text(String.format("%s_%s_%05d", lang, uidHash, seqNum));
  }

  // We don't want to keep all info from the original event objects, and we discard it here.
  private static void sparsifyJson(JSONObject json, boolean isGlobalRoot) {
    // First process children recursively.
    if (json.has(JSON_CHILDREN)) {
      JSONArray children = json.getJSONArray(JSON_CHILDREN);
      for (int i = 0; i < children.length(); ++i) {
        sparsifyJson((JSONObject) children.get(i), false);
      }
    }
    // Then process the root itself.
    for (String field : JSONObject.getNames(json)) {
      // Remove all superfluous fields. Certain fields are only removed for non-root nodes.
      if (!FIELDS_TO_KEEP.contains(field)
          && !(isGlobalRoot && FIELDS_TO_KEEP_IN_ROOT.contains(field))) {
        json.remove(field);
      }
    }
  }

  protected boolean isGoodEvent(JSONObject root, boolean isGlobalRoot, Reporter reporter,
      String lang) throws JSONException {
    // If the root of the tree has no referer and no children, we don't know if this browser sends
    // referer info, so we exclude the tree.
    if (isGlobalRoot && !root.has(JSON_CHILDREN) && !keepSingletonTrees) {
      reporter.incrCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_SINGLETON, 1);
      return false;
    }
    // The root must not be a Wikimedia page; this will discard traces that in fact continue a
    // previous tree (e.g., one whose parent was discarded in the mapper because it doesn't match
    // the required URL pattern). As an exception, we do allow trees that start with the home page
    // of one of the specified Wikipedia versions.
    if (isGlobalRoot && WIKI_HOST_PATTERN.matcher(root.getString(JSON_REFERER)).matches()
        && !homePagePattern.matcher(root.getString(JSON_REFERER)).matches()) {
      reporter.incrCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_WIKIMEDIA_REFERER_IN_ROOT, 1);
      return false;
    }
    // If we don't want to keep ambiguous trees, discard them.
    if (!keepAmbiguousTrees && root.has(JSON_PARENT_AMBIGUOUS)
        && root.getBoolean(JSON_PARENT_AMBIGUOUS)) {
      reporter.incrCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_AMBIGUOUS, 1);
      return false;
    }
    return true;
  }

  // Depth-first search, failing as soon as a node fails.
  protected boolean isGoodTree(JSONObject root, int recursionDepth, Reporter reporter, String lang)
      throws JSONException {
    if (recursionDepth > MAX_RECURSION_DEPTH) {
      reporter.incrCounter(HADOOP_COUNTERS.REDUCE_STACKOVERFLOW, 1);
      return false;
    } else if (!isGoodEvent(root, recursionDepth == 0, reporter, lang)) {
      return false;
    } else if (root.has(JSON_CHILDREN)) {
      JSONArray children = root.getJSONArray(JSON_CHILDREN);
      for (int i = 0; i < children.length(); ++i) {
        if (!isGoodTree(children.getJSONObject(i), recursionDepth + 1, reporter, lang)) {
          return false;
        }
      }
    }
    return true;
  }

  // Keep only the good trees. As a side effect, this sparsifies the JSON objects.
  private List<BrowserEvent> filterTrees(List<BrowserEvent> roots, Reporter reporter, String lang) {
    List<BrowserEvent> filtered = new ArrayList<BrowserEvent>();
    for (BrowserEvent root : roots) {
      try {
        if (isGoodTree(root.json, 0, reporter, lang)) {
          sparsifyJson(root.json, true);
          filtered.add(root);
        } else if (keepBadTrees) {
          sparsifyJson(root.json, true);
          root.json.put(JSON_BAD_TREE, true);
          filtered.add(root);
        }
      } catch (JSONException e) {
        System.out.format("%s\n", e.getMessage());
      }
    }
    return filtered;
  }

  // Input: the list of session browser events in temporal order.
  // Output: the list of tree roots.
  private List<BrowserEvent> getMinimumSpanningForest(List<BrowserEvent> session, Reporter reporter)
      throws JSONException {
    // The list of all trees contained in the session.
    List<BrowserEvent> roots = new ArrayList<BrowserEvent>();
    // Count for all URLs how often they were seen in this session.
    Map<String, Integer> pageCounts = new HashMap<String, Integer>();
    // Remember, for each page, the last event in which it was seen.
    Map<String, BrowserEvent> pageToLastEvent = new HashMap<String, BrowserEvent>();
    // Iterate over all events in temporal order.
    for (BrowserEvent event : session) {
      String ref = event.getRefererPathAndQuery();
      BrowserEvent parent = pageToLastEvent.get(ref);
      // If we haven't seen the referer of this event in the current session, make the event a
      // root.
      if (parent == null) {
        roots.add(event);
      }
      // Otherwise, append it as a child to the latest event involving the referer page.
      else {
        parent.json.append(JSON_CHILDREN, event.json);
        Integer refererCount = pageCounts.get(ref);
        // A parent is ambiguous if we have seen the referer URL several times in this session
        // before the current event.
        if (refererCount != null && refererCount > 1) {
          event.json.put(JSON_PARENT_AMBIGUOUS, true);
        }
      }
      // Remember this event as the last event for its URL.
      pageToLastEvent.put(event.getPathAndQuery(), event);
      // Update the counter for this URL.
      Integer c = pageCounts.get(event.getPathAndQuery());
      pageCounts.put(event.getPathAndQuery(), c == null ? 1 : c + 1);
    }
    return roots;
  }

  // Takes a list of events, orders them by time, and extracts a set of trees via the
  // minimum-spanning-forest heuristic.
  public List<BrowserEvent> sequenceToTrees(List<BrowserEvent> events, Reporter reporter)
      throws JSONException {
    // This sort is stable, so requests having the same timestamp will stay in the original order.
    Collections.sort(events, new Comparator<BrowserEvent>() {
      @Override
      public int compare(BrowserEvent pv1, BrowserEvent pv2) {
        long t1 = pv1.time;
        long t2 = pv2.time;
        // NB: Don't use subtraction-based comparison, since overflow may cause errors!
        if (t1 > t2)
          return 1;
        else if (t1 < t2)
          return -1;
        else
          return 0;
      }
    });
    return getMinimumSpanningForest(events, reporter);
  }

  private void setDefaultConfig() {
    homePagePattern = Pattern.compile("http.?://pt\\.wikipedia\\.org/");
    keepAmbiguousTrees = true;
    keepBadTrees = true;
    keepSingletonTrees = true;
    hashSalt = "sdsdsafdsfdsfs";
    maxNumEvents = 3600;
  }

  @Override
  public void configure(JobConf conf) {
    if (conf == null) {
      setDefaultConfig();
    } else {
      homePagePattern = Pattern.compile("http.?://(" + conf.get(CONF_LANGUAGE_PATTERN, "")
          + ")\\.wikipedia\\.org/");
      keepAmbiguousTrees = conf.getBoolean(CONF_KEEP_AMBIGUOUS_TREES, true);
      keepBadTrees = conf.getBoolean(CONF_KEEP_BAD_TREES, false);
      keepSingletonTrees = conf.getBoolean(CONF_KEEP_SINGLETON_TREES, false);
      hashSalt = conf.get(CONF_HASH_SALT);
      maxNumEvents = conf.getInt(CONF_MAX_NUM_EVENTS, 100000);
      String[] languages = conf.get(CONF_LANGUAGE_PATTERN, "").split("\\|");
      readAllRedirects(languages);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void reduce(Text key, Iterator<Text> eventIterator, OutputCollector<Text, Text> out,
      Reporter reporter) throws IOException {
    try {
      List<BrowserEvent> events = new ArrayList<BrowserEvent>();
      int n = 0;
      String[] lang_day_uid = key.toString().split(GroupAndFilterMapper.UID_SEPARATOR, 2);
      String lang = lang_day_uid[0];
      String uid = lang_day_uid[1];

      // Collect all events for this user on this day.
      while (eventIterator.hasNext()) {
        // If there are too many event events, output nothing.
        if (++n > maxNumEvents) {
          reporter.incrCounter(HADOOP_COUNTERS.REDUCE_TOO_MANY_EVENTS, 1);
          return;
        } else {
          JSONObject json = new JSONObject(eventIterator.next().toString());
          long before = System.currentTimeMillis();
          BrowserEvent event = BrowserEvent.newInstance(json, redirects.get(lang));
          long after = System.currentTimeMillis();
          reporter.incrCounter(HADOOP_COUNTERS.REDUCE_MSEC_EVENT_CONSTRUCTOR, after - before);
          events.add(event);
          if (!json.has(JSON_UNRESOLVED_TITLE)) {
            reporter.incrCounter(HADOOP_COUNTERS.REDUCE_REDIRECT_RESOLVED, 1);
          }
        }
      }
      // Extract trees and output them.
      List<BrowserEvent> allRoots = sequenceToTrees(events, reporter);
      List<BrowserEvent> goodRoots = filterTrees(allRoots, reporter, lang);
      int i = 0;
      for (BrowserEvent root : goodRoots) {
        root.json.put(JSON_TREE_ID, makeTreeId(lang, uid, i));
        out.collect(new Text(lang), new Text(root.toString()));
        if (root.json.has(JSON_BAD_TREE) && root.json.getBoolean(JSON_BAD_TREE)) {
          reporter.incrCounter(HADOOP_COUNTERS.REDUCE_BAD_TREE, 1);
        } else {
          reporter.incrCounter(HADOOP_COUNTERS.REDUCE_OK_TREE, 1);
        }
        ++i;
      }
    } catch (Exception e) {
      reporter.incrCounter(HADOOP_COUNTERS.REDUCE_EXCEPTION, 1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
