package org.wikimedia.west1.traces;

import java.io.IOException;
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
import java.util.Set;
import java.util.regex.Pattern;

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

	// Having 3600 pageviews in a day would mean one every 24 seconds, a lot...
	private static final int MAX_NUM_PAGEVIEWS = 3600;
	// JSON field names.
	private static final String JSON_CHILDREN = "children";
	private static final String JSON_PARENT_AMBIGUOUS = "parent_ambiguous";
	private static final String JSON_BAD_TREE = "bad_tree";
	private static final String JSON_DT = "dt";
	private static final String JSON_UA = "user_agent";
	private static final String JSON_URI_PATH = "uri_path";
	private static final String JSON_URI_HOST = "uri_host";
	private static final String JSON_CONTENT_TYPE = "content_type";
	private static final String JSON_URI_QUERY = "uri_query";
	private static final String JSON_HTTP_STATUS = "http_status";
	private static final String JSON_REFERER = "referer";
	// Job config parameters specifying which Wikipedia versions we're interested in, e.g.,
	// "(pt|es)\\.wikipedia\\.org".
	private static final String CONF_URI_HOST_PATTERN = "org.wikimedia.west1.traces.uriHostPattern";
	// Job config parameters specifying if we want to keep trees in which at least one node has an
	// ambiguous parent (the algorithm will always pick the temporally closest one).
	private static final String CONF_KEEP_AMBIGUOUS_TREES = "org.wikimedia.west1.traces.keepAmbiguousTrees";
	// Job config parameters specifying if we want to keep trees whose root is from a Wikimedia site.
	private static final String CONF_KEEP_BAD_TREES = "org.wikimedia.west1.traces.keepBadTrees";
	// Job config parameters specifying a string for salting UID hashes, so it's very hard to get the
	// hash value for a given UID.
	private static final String CONF_HASH_SALT = "org.wikimedia.west1.traces.hashSalt";
	// A pattern matching Wikimedia host names.
	private static final Pattern WIKI_HOST_PATTERN = Pattern.compile("[a-z]+://[^/]*wiki.*");

	// The fields you want to store for every pageview.
	private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(
	    JSON_CONTENT_TYPE, JSON_DT, JSON_URI_PATH, JSON_URI_QUERY, JSON_HTTP_STATUS, JSON_REFERER,
	    // The fields we added.
	    JSON_CHILDREN, JSON_PARENT_AMBIGUOUS, JSON_BAD_TREE));
	// The fields you want to store only for the root (because they're identical for all pageviews in
	// the same tree).
	private static final Set<String> FIELDS_TO_KEEP_IN_ROOT = new HashSet<String>(Arrays.asList(
	    JSON_URI_HOST, JSON_UA));

	private static enum HADOOP_COUNTERS {
		OK_TREES, FILTERED_TREES, REDUCE_ERROR
	}

	private Pattern mainPagePattern;
	private boolean keepAmbiguousTrees;
	private boolean keepBadTrees;
	private String hashSalt;

	// Tree ids consist of the day, a salted hash of the UID, and a sequential number (in order of
	// time), e.g., 20150118_5ca697716da3203201f56d09b41c954d_0004.
	private Text makeTreeId(Text dayAndUid, int seqNum) {
		String[] day_uid = dayAndUid.toString().split(GroupAndFilterMapper.UID_SEPARATOR, 2);
		String day = day_uid[0];
		String uidHash = DigestUtils.md5Hex(day_uid[1] + hashSalt);
		// seqNum is zero-padded to fixed length 4: we allow at most MAX_NUM_PAGEVIEWS = 3600 pageviews
		// per day, and in the worst case, each pageview is its own tree, so seqNum <= 3600.
		return new Text(String.format("%s_%s_%04d", day.replace("-", ""), uidHash, seqNum));
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

	protected boolean isGoodPageview(JSONObject root, boolean isGlobalRoot) throws JSONException {
		return
		// No node can be from the last hour of the day, such that we make trees spanning the day
		// boundary extremely unlikely (they'd have to include an idle-time of at least one hour; if
		// that ever happens, we consider the part before the idle-time a complete tree).
		!root.getString("dt").contains("T23:")
		// The root must not be a Wikimedia page; this will discard traces that in fact continue a
		// previous tree (e.g., one that starts before the day boundary and whose first part was
		// excluded via the "T23:" rule; or one whose parent was discarded in the mapper because it
		// doesn't match the required URL pattern). As an exception, we do allow trees that start with
		// the main page of one of the specified Wikipedia versions.
		    && (!isGlobalRoot || !WIKI_HOST_PATTERN.matcher(root.getString("referer")).matches() || mainPagePattern
		        .matcher(root.getString("referer")).matches())
		    // If we don't want to keep ambiguous trees, discard them.
		    && (keepAmbiguousTrees || !root.getBoolean(JSON_PARENT_AMBIGUOUS));
	}

	// Depth-first search, failing as soon as a node fails.
	protected boolean isGoodTree(JSONObject root, boolean isGlobalRoot) throws JSONException {
		if (!isGoodPageview(root, isGlobalRoot)) {
			return false;
		} else if (root.has(JSON_CHILDREN)) {
			JSONArray children = root.getJSONArray(JSON_CHILDREN);
			for (int i = 0; i < children.length(); ++i) {
				if (!isGoodTree(children.getJSONObject(i), false)) {
					return false;
				}
			}
		}
		return true;
	}

	// Keep only the good trees.
	private List<Pageview> filterTrees(List<Pageview> roots) {
		List<Pageview> filtered = new ArrayList<Pageview>();
		for (Pageview root : roots) {
			try {
				if (isGoodTree(root.json, true)) {
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
	private static List<Pageview> getMinimumSpanningForest(List<Pageview> session)
	    throws JSONException {
		// The list of all trees contained in the session.
		List<Pageview> roots = new ArrayList<Pageview>();
		// Count for all URLs how often they were seen in this session.
		Map<String, Integer> urlCounts = new HashMap<String, Integer>();
		// Remember, for each URL, the last pageview of it.
		Map<String, Pageview> urlToLastPageview = new HashMap<String, Pageview>();
		// Iterate over all pageviews in temporal order.
		for (Pageview pv : session) {
			Pageview parent = urlToLastPageview.get(pv.referer);
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
				pv.json.put(JSON_PARENT_AMBIGUOUS, urlCounts.get(pv.referer) > 1);
			}
			// Remember this pageview as the last pageview for its URL.
			urlToLastPageview.put(pv.url, pv);
			// Update the counter for this URL.
			Integer c = urlCounts.get(pv.url);
			urlCounts.put(pv.url, c == null ? 1 : c + 1);
		}
		return roots;
	}

	// Takes a list of pageviews, orders them by time, and extracts a set of trees via the
	// minimum-spanning-forest heuristic.
	public List<Pageview> sequenceToTrees(List<Pageview> pageviews) throws JSONException,
	    ParseException {
		// This sort is stable, so requests having the same timestamp will stay in the original order.
		Collections.sort(pageviews, new Comparator<Pageview>() {
			@Override
			public int compare(Pageview pv1, Pageview pv2) {
				return (int) (pv1.time - pv2.time);
			}
		});
		return getMinimumSpanningForest(pageviews);
	}

	private void setDefaultConfig() {
		mainPagePattern = Pattern.compile("http.?://pt\\.wikipedia\\.org/");
		keepAmbiguousTrees = true;
		keepBadTrees = true;
		hashSalt = "sdsdsafdsfdsfs";
	}

	@Override
	public void configure(JobConf conf) {
		if (conf == null) {
			setDefaultConfig();
		} else {
			mainPagePattern = Pattern.compile("http.?://(" + conf.get(CONF_URI_HOST_PATTERN, "") + ")/");
			keepAmbiguousTrees = conf.getBoolean(CONF_KEEP_AMBIGUOUS_TREES, true);
			keepBadTrees = conf.getBoolean(CONF_KEEP_BAD_TREES, false);
			hashSalt = conf.get(CONF_HASH_SALT);
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void reduce(Text dayAndUid, Iterator<Text> pageviewIterator,
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
			List<Pageview> allRoots = sequenceToTrees(pageviews);
			List<Pageview> goodRoots = filterTrees(allRoots);
			reporter.incrCounter(HADOOP_COUNTERS.FILTERED_TREES, allRoots.size() - goodRoots.size());
			int i = 0;
			for (Pageview root : goodRoots) {
				out.collect(makeTreeId(dayAndUid, i), new Text(root.toString()));
				reporter.incrCounter(HADOOP_COUNTERS.OK_TREES, 1);
				++i;
			}
		} catch (Exception e) {
			reporter.incrCounter(HADOOP_COUNTERS.REDUCE_ERROR, 1);
			System.err.format("%s\n", e.getMessage());
		}
	}

}
