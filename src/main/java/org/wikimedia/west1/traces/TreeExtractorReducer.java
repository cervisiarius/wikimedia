package org.wikimedia.west1.traces;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.ParseException;
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
  private static final String JSON_URI_PATH = "uri_path";
  private static final String JSON_HTTP_STATUS = "http_status";
  private static final String JSON_REFERER = "referer";
  private static final String JSON_RESOLVED_URI_PATH = "resolved_uri_path";
  // Job config parameters specifying which Wikipedia versions we're interested in, e.g.,
  // "(pt|es)\\.wikipedia\\.org".
  private static final String CONF_LANGUAGE_PATTERN = "org.wikimedia.west1.traces.languagePattern";
  // Job config parameters specifying if we want to keep trees in which at least one node has an
  // ambiguous parent (the algorithm will always pick the temporally closest one).
  private static final String CONF_KEEP_AMBIGUOUS_TREES = "org.wikimedia.west1.traces.keepAmbiguousTrees";
  // Job config parameters specifying if we want to keep trees whose root is from a Wikimedia site.
  private static final String CONF_KEEP_BAD_TREES = "org.wikimedia.west1.traces.keepBadTrees";
  // Job config parameters specifying if we want to keep trees consisting of a single pageview (even
  // if this is true, we keep only the singletons that have a non-empty referer, so we can be sure
  // the user's browser sends referer information).
  private static final String CONF_KEEP_SINGLETON_TREES = "org.wikimedia.west1.traces.keepSingletonTrees";
  // Job config parameters specifying a string for salting UID hashes, so it's very hard to get the
  // hash value for a given UID.
  private static final String CONF_HASH_SALT = "org.wikimedia.west1.traces.hashSalt";
  // If a user has more than this many pageviews, we ignore her.
  private static final String CONF_MAX_NUM_PAGEVIEWS = "org.wikimedia.west1.traces.maxNumPageviews";
  // A pattern matching Wikimedia host names.
  private static final Pattern WIKI_HOST_PATTERN = Pattern.compile("[a-z]+://[^/]*"
  // Adapted from
  // https://github.com/wikimedia/analytics-refinery-source/blob/master/refinery-core/src/...
  // .../main/java/org/wikimedia/analytics/refinery/core/Pageview.java.
  // + "wik(ibooks|idata|inews|imedia|ipedia|iquote|isource|tionary|iversity|ivoyage)\\.org.*";
      + "wiki(data|media|pedia)\\.org.*");

  // The fields you want to store for every pageview.
  private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(JSON_DT,
      JSON_URI_PATH, JSON_HTTP_STATUS,
      // The fields we added.
      JSON_RESOLVED_URI_PATH, JSON_CHILDREN, JSON_PARENT_AMBIGUOUS, JSON_BAD_TREE));
  // The fields you want to store only for the root (because they're identical for all pageviews in
  // the same tree).
  private static final Set<String> FIELDS_TO_KEEP_IN_ROOT = new HashSet<String>(Arrays.asList(
      JSON_UA, JSON_REFERER, JSON_TREE_ID));

  private static enum HADOOP_COUNTERS {
    // In order to have an idea what the big reasons are for dismissing trees. Note that these don't
    // add up to the number of trees we start with before filtering, since filtering fails fast (cf.
    // isGoodPageview).
    REDUCE_SKIPPED_SINGLETON, REDUCE_SKIPPED_HOUR_23, REDUCE_SKIPPED_WIKIMEDIA_REFERER_IN_ROOT, REDUCE_SKIPPED_AMBIGUOUS, REDUCE_TOO_MANY_PAGEVIEWS,
    // OK_TREE and BAD_TREE add to the number of trees we started with before filtering.
    REDUCE_OK_TREE, REDUCE_BAD_TREE, REDUCE_EXCEPTION, REDUCE_REDIRECT_RESOLVED,
    // Timers.
    REDUCE_MSEC_PAGEVIEW_CONSTRUCTOR
  }

  private Pattern homePagePattern;
  private boolean keepAmbiguousTrees;
  private boolean keepBadTrees;
  private boolean keepSingletonTrees;
  private String hashSalt;
  private int maxNumPageviews;
  private String[] languages;
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

  private void readAllRedirects() {
    for (String lang : languages) {
      String file = lang + "_redirects.tsv.gz";
      try {
        InputStream is = new GZIPInputStream(ClassLoader.getSystemResourceAsStream(file));
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
    // seqNum is zero-padded to fixed length 5: we allow at most MAX_NUM_PAGEVIEWS = 100K pageviews
    // per day, and in the worst case, each pageview is its own tree, so seqNum <= 99999.
    return new Text(String.format("%s_%s_%05d", lang, uidHash, seqNum));
  }

  // We don't want to keep all info from the original pageview objects, and we discard it here.
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

  protected boolean isGoodPageview(JSONObject root, boolean isGlobalRoot, Reporter reporter,
      String lang) throws JSONException {
    // If the root of the tree has no referer and no children, we don't know if this browser sends
    // referer info, so we exclude the tree.
    if (isGlobalRoot && !root.has(JSON_CHILDREN)
        && (!keepSingletonTrees || !root.getString(JSON_REFERER).startsWith("http"))) {
      reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_SKIPPED_SINGLETON.toString(), 1);
      return false;
    }
    // The root must not be a Wikimedia page; this will discard traces that in fact continue a
    // previous tree (e.g., one that starts before the day boundary and whose first part was
    // excluded via the "T23:" rule; or one whose parent was discarded in the mapper because it
    // doesn't match the required URL pattern). As an exception, we do allow trees that start with
    // the main page of one of the specified Wikipedia versions.
    if (isGlobalRoot && WIKI_HOST_PATTERN.matcher(root.getString(JSON_REFERER)).matches()
        && !homePagePattern.matcher(root.getString(JSON_REFERER)).matches()) {
      reporter.incrCounter(lang,
          HADOOP_COUNTERS.REDUCE_SKIPPED_WIKIMEDIA_REFERER_IN_ROOT.toString(), 1);
      return false;
    }
    // If we don't want to keep ambiguous trees, discard them.
    if (!keepAmbiguousTrees && root.getBoolean(JSON_PARENT_AMBIGUOUS)) {
      reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_SKIPPED_AMBIGUOUS.toString(), 1);
      return false;
    }
    // No node can be from the last hour of the day, such that we make trees spanning the day
    // boundary extremely unlikely (they'd have to include an idle-time of at least one hour; if
    // that ever happens, we consider the part before the idle-time a complete tree).
    // if (root.getString(JSON_DT).contains("T23:")) {
    // reporter.incrCounter(lang, HADOOP_COUNTERS.SKIPPED_HOUR_23.toString(), 1);
    // return false;
    // }
    return true;
  }

  // Depth-first search, failing as soon as a node fails.
  protected boolean isGoodTree(JSONObject root, boolean isGlobalRoot, Reporter reporter, String lang)
      throws JSONException {
    if (!isGoodPageview(root, isGlobalRoot, reporter, lang)) {
      return false;
    } else if (root.has(JSON_CHILDREN)) {
      JSONArray children = root.getJSONArray(JSON_CHILDREN);
      for (int i = 0; i < children.length(); ++i) {
        if (!isGoodTree(children.getJSONObject(i), false, reporter, lang)) {
          return false;
        }
      }
    }
    return true;
  }

  // Keep only the good trees.
  private List<Pageview> filterTrees(List<Pageview> roots, Reporter reporter, String lang) {
    List<Pageview> filtered = new ArrayList<Pageview>();
    for (Pageview root : roots) {
      try {
        if (isGoodTree(root.json, true, reporter, lang)) {
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

  // Input: the list of session pageviews in temporal order.
  // Output: the list of tree roots.
  private List<Pageview> getMinimumSpanningForest(List<Pageview> session, Reporter reporter)
      throws JSONException {
    // The list of all trees contained in the session.
    List<Pageview> roots = new ArrayList<Pageview>();
    // Count for all URLs how often they were seen in this session.
    Map<String, Integer> articleCounts = new HashMap<String, Integer>();
    // Remember, for each URL, the last pageview of it.
    Map<String, Pageview> articleToLastPageview = new HashMap<String, Pageview>();
    // Iterate over all pageviews in temporal order.
    for (Pageview pv : session) {
      Pageview parent = articleToLastPageview.get(pv.refererArticle);
      // If we haven't seen the referer of this pageview in the current session, make the pageview a
      // root.
      if (parent == null) {
        roots.add(pv);
        pv.json.put(JSON_PARENT_AMBIGUOUS, false);
      }
      // Otherwise, append it as a child to the latest pageview of the referer.
      else {
        parent.json.append(JSON_CHILDREN, pv.json);
        // A parent is ambiguous if we have seen the referer URL several times in this session
        // before the current pageview.
        Integer refererCount = articleCounts.get(pv.refererArticle);
        if (refererCount == null) {
          refererCount = 0;
        }
        pv.json.put(JSON_PARENT_AMBIGUOUS, refererCount > 1);
      }
      // Remember this pageview as the last pageview for its URL.
      articleToLastPageview.put(pv.resolvedArticle, pv);
      // Update the counter for this URL.
      Integer c = articleCounts.get(pv.resolvedArticle);
      articleCounts.put(pv.resolvedArticle, c == null ? 1 : c + 1);
    }
    return roots;
  }

  // Takes a list of pageviews, orders them by time, and extracts a set of trees via the
  // minimum-spanning-forest heuristic.
  public List<Pageview> sequenceToTrees(List<Pageview> pageviews, Reporter reporter)
      throws JSONException, ParseException {
    // This sort is stable, so requests having the same timestamp will stay in the original order.
    Collections.sort(pageviews, new Comparator<Pageview>() {
      @Override
      public int compare(Pageview pv1, Pageview pv2) {
        return (int) (pv1.time - pv2.time);
      }
    });
    return getMinimumSpanningForest(pageviews, reporter);
  }

  private void setDefaultConfig() {
    homePagePattern = Pattern.compile("http.?://pt\\.wikipedia\\.org/");
    keepAmbiguousTrees = true;
    keepBadTrees = true;
    keepSingletonTrees = true;
    hashSalt = "sdsdsafdsfdsfs";
    maxNumPageviews = 3600;
    languages = new String[] { "pt" };
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
      maxNumPageviews = conf.getInt(CONF_MAX_NUM_PAGEVIEWS, 100000);
      languages = conf.get(CONF_LANGUAGE_PATTERN, "").split("\\|");
      readAllRedirects();
    }
  }

  @Override
  public void close() throws IOException {
  }

  private boolean memoryCounterSet = false;

  @Override
  public void reduce(Text key, Iterator<Text> pageviewIterator, OutputCollector<Text, Text> out,
      Reporter reporter) throws IOException {
    if (!memoryCounterSet) {
      Runtime runtime = Runtime.getRuntime();
      // long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
      // Max memory is 4GB.
      reporter.incrCounter("Global counters", "REDUCE_MAX_MEMORY", runtime.maxMemory());
      memoryCounterSet = true;
    }
    try {
      List<Pageview> pageviews = new ArrayList<Pageview>();
      int n = 0;
      String[] lang_day_uid = key.toString().split(GroupAndFilterMapper.UID_SEPARATOR, 2);
      String lang = lang_day_uid[0];
      String uid = lang_day_uid[1];

      // Collect all pageviews for this user on this day.
      while (pageviewIterator.hasNext()) {
        // If there are too many pageview events, output nothing.
        if (++n > maxNumPageviews) {
          reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_TOO_MANY_PAGEVIEWS.toString(), 1);
          return;
        } else {
          JSONObject json = new JSONObject(pageviewIterator.next().toString());
          long before = System.currentTimeMillis();
          Pageview pv = new Pageview(json, redirects.get(lang));
          long after = System.currentTimeMillis();
          reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_MSEC_PAGEVIEW_CONSTRUCTOR.toString(),
              after - before);
          pageviews.add(pv);
          if (!json.getString(JSON_URI_PATH).equals(pv.resolvedArticle)) {
            reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_REDIRECT_RESOLVED.toString(), 1);
          }
        }
      }
      // Extract trees and output them.
      List<Pageview> allRoots = sequenceToTrees(pageviews, reporter);
      List<Pageview> goodRoots = filterTrees(allRoots, reporter, lang);
      int i = 0;
      for (Pageview root : goodRoots) {
        root.json.put(JSON_TREE_ID, makeTreeId(lang, uid, i));
        out.collect(new Text(lang), new Text(root.toString()));
        if (root.json.has(JSON_BAD_TREE) && root.json.getBoolean(JSON_BAD_TREE)) {
          reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_BAD_TREE.toString(), 1);
        } else {
          reporter.incrCounter(lang, HADOOP_COUNTERS.REDUCE_OK_TREE.toString(), 1);
        }
        ++i;
      }
    } catch (Exception e) {
      reporter.incrCounter("Global counters", HADOOP_COUNTERS.REDUCE_EXCEPTION.toString(), 1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
