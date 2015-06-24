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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractorReducer extends Reducer<Text, Text, NullWritable, Text> {

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
  private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(
      BrowserEvent.JSON_DT, BrowserEvent.JSON_TITLE,
      BrowserEvent.JSON_HTTP_STATUS,
      // The fields we added.
      BrowserEvent.JSON_ANCHOR, BrowserEvent.JSON_UNRESOLVED_TITLE, BrowserEvent.JSON_CHILDREN,
      BrowserEvent.JSON_PARENT_AMBIGUOUS, BrowserEvent.JSON_BAD_TREE, BrowserEvent.JSON_IS_SEARCH,
      BrowserEvent.JSON_SEARCH_PARAMS));
  // The fields you want to store only for the root (because they're identical for all events in
  // the same tree).
  private static final Set<String> FIELDS_TO_KEEP_IN_ROOT = new HashSet<String>(Arrays.asList(
      BrowserEvent.JSON_UA, BrowserEvent.JSON_REFERER, BrowserEvent.JSON_TREE_ID));

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

  private MultipleOutputs<NullWritable, Text> out;

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

  // Tree ids consist of the language, a salted hash of the UID, the day, and a sequential number
  // (in order of time), e.g., de_5ca697716da3203201f56d09b41c954d_20150118_0004.
  private Text makeTreeId(String lang, String uid, String dt, int seqNum) {
    String uidHash = DigestUtils.md5Hex(uid + hashSalt);
    String day = dt.substring(0, dt.indexOf('T')).replace("-", "");
    // seqNum is zero-padded to fixed length 4: if we allow at most MAX_NUM_EVENTS = 10K events
    // per day, and in the worst case, each event is its own tree, then seqNum <= 9999.
    return new Text(String.format("%s_%s_%s_%04d", lang, uidHash, day, seqNum));
  }

  private static String generateOutputFilename(String lang) {
    return lang + "/part";
  }

  // We don't want to keep all info from the original event objects, and we discard it here.
  private static void sparsifyJson(JSONObject json, boolean isGlobalRoot) {
    // First process children recursively.
    if (json.has(BrowserEvent.JSON_CHILDREN)) {
      JSONArray children = json.getJSONArray(BrowserEvent.JSON_CHILDREN);
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

  protected boolean isGoodEvent(JSONObject root, boolean isGlobalRoot,
      Reducer<Text, Text, NullWritable, Text>.Context context, String lang) throws JSONException {
    // If the root of the tree has no referer and no children, we don't know if this browser sends
    // referer info, so we exclude the tree.
    if (isGlobalRoot && !root.has(BrowserEvent.JSON_CHILDREN) && !keepSingletonTrees) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_SINGLETON).increment(1);
      return false;
    }
    // The root must not be a Wikimedia page; this will discard traces that in fact continue a
    // previous tree (e.g., one whose parent was discarded in the mapper because it doesn't match
    // the required URL pattern). As an exception, we do allow trees that start with the home page
    // of one of the specified Wikipedia versions.
    if (isGlobalRoot
        && WIKI_HOST_PATTERN.matcher(root.getString(BrowserEvent.JSON_REFERER)).matches()
        && !homePagePattern.matcher(root.getString(BrowserEvent.JSON_REFERER)).matches()) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_WIKIMEDIA_REFERER_IN_ROOT).increment(1);
      return false;
    }
    // If we don't want to keep ambiguous trees, discard them.
    if (!keepAmbiguousTrees && root.has(BrowserEvent.JSON_PARENT_AMBIGUOUS)
        && root.getBoolean(BrowserEvent.JSON_PARENT_AMBIGUOUS)) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_SKIPPED_AMBIGUOUS).increment(1);
      return false;
    }
    return true;
  }

  // Depth-first search, failing as soon as a node fails.
  protected boolean isGoodTree(JSONObject root, int recursionDepth,
      Reducer<Text, Text, NullWritable, Text>.Context context, String lang) throws JSONException {
    if (recursionDepth > MAX_RECURSION_DEPTH) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_STACKOVERFLOW).increment(1);
      return false;
    } else if (!isGoodEvent(root, recursionDepth == 0, context, lang)) {
      return false;
    } else if (root.has(BrowserEvent.JSON_CHILDREN)) {
      JSONArray children = root.getJSONArray(BrowserEvent.JSON_CHILDREN);
      for (int i = 0; i < children.length(); ++i) {
        if (!isGoodTree(children.getJSONObject(i), recursionDepth + 1, context, lang)) {
          return false;
        }
      }
    }
    return true;
  }

  // Keep only the good trees. As a side effect, this sparsifies the JSON objects.
  private List<BrowserEvent> filterTrees(List<BrowserEvent> roots,
      Reducer<Text, Text, NullWritable, Text>.Context context, String lang) {
    List<BrowserEvent> filtered = new ArrayList<BrowserEvent>();
    for (BrowserEvent root : roots) {
      try {
        if (isGoodTree(root.json, 0, context, lang)) {
          sparsifyJson(root.json, true);
          filtered.add(root);
        } else if (keepBadTrees) {
          sparsifyJson(root.json, true);
          root.json.put(BrowserEvent.JSON_BAD_TREE, true);
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
  private List<BrowserEvent> getMinimumSpanningForest(List<BrowserEvent> session)
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
        parent.json.append(BrowserEvent.JSON_CHILDREN, event.json);
        Integer refererCount = pageCounts.get(ref);
        // A parent is ambiguous if we have seen the referer URL several times in this session
        // before the current event.
        if (refererCount != null && refererCount > 1) {
          event.json.put(BrowserEvent.JSON_PARENT_AMBIGUOUS, true);
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
  public List<BrowserEvent> sequenceToTrees(List<BrowserEvent> events) throws JSONException {
    // This sort is stable, so requests having the same timestamp will stay in the original order.
    Collections.sort(events, new Comparator<BrowserEvent>() {
      @Override
      public int compare(BrowserEvent e1, BrowserEvent e2) {
        long t1 = e1.time;
        long t2 = e2.time;
        // NB: Don't use subtraction-based comparison, since overflow may cause errors!
        if (t1 > t2)
          return 1;
        else if (t1 < t2)
          return -1;
        else
          return 0;
      }
    });
    return getMinimumSpanningForest(events);
  }

  private void setDefaultConfig() {
    homePagePattern = Pattern.compile("http.?://pt\\.wikipedia\\.org/");
    keepAmbiguousTrees = true;
    keepBadTrees = true;
    keepSingletonTrees = true;
    hashSalt = "sdsdsafdsfdsfs";
    maxNumEvents = 3600;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void setup(Reducer<Text, Text, NullWritable, Text>.Context context) {
    if (context == null) {
      setDefaultConfig();
    } else {
      out = new MultipleOutputs(context);
      Configuration conf = context.getConfiguration();
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
  protected void cleanup(Context context) throws IOException, InterruptedException {
    out.close();
  }

  @Override
  public void reduce(Text key, Iterable<Text> eventsIt,
      Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException {
    try {
      List<BrowserEvent> events = new ArrayList<BrowserEvent>();
      int n = 0;
      String[] lang_uid = key.toString().split(GroupAndFilterMapper.UID_SEPARATOR, 2);
      String lang = lang_uid[0];
      String uid = lang_uid[1];

      // Collect all events for this user on this day.
      for (Text eventText : eventsIt) {
        // If there are too many event events, output nothing.
        if (++n > maxNumEvents) {
          context.getCounter(HADOOP_COUNTERS.REDUCE_TOO_MANY_EVENTS).increment(1);
          return;
        } else {
          JSONObject json = new JSONObject(eventText.toString());
          long before = System.currentTimeMillis();
          BrowserEvent event = BrowserEvent.newInstance(json, redirects.get(lang));
          long after = System.currentTimeMillis();
          context.getCounter(HADOOP_COUNTERS.REDUCE_MSEC_EVENT_CONSTRUCTOR).increment(
              after - before);
          events.add(event);
          if (json.has(BrowserEvent.JSON_UNRESOLVED_TITLE)) {
            context.getCounter(HADOOP_COUNTERS.REDUCE_REDIRECT_RESOLVED).increment(1);
          }
        }
      }
      // Extract trees and output them.
      List<BrowserEvent> allRoots = sequenceToTrees(events);
      List<BrowserEvent> goodRoots = filterTrees(allRoots, context, lang);
      int i = 0;
      for (BrowserEvent root : goodRoots) {
        root.json.put(BrowserEvent.JSON_TREE_ID,
            makeTreeId(lang, uid, root.json.getString(BrowserEvent.JSON_DT), i));
        // Write the output to the folder for this language.
        out.write(NullWritable.get(), new Text(root.toString()), generateOutputFilename(lang));
        if (root.json.has(BrowserEvent.JSON_BAD_TREE)
            && root.json.getBoolean(BrowserEvent.JSON_BAD_TREE)) {
          context.getCounter(HADOOP_COUNTERS.REDUCE_BAD_TREE).increment(1);
        } else {
          context.getCounter(HADOOP_COUNTERS.REDUCE_OK_TREE).increment(1);
        }
        ++i;
      }
    } catch (Exception e) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_EXCEPTION).increment(1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
