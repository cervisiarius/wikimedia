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
	private static final String JSON_CHILDREN = "children";
	private static final String JSON_PARENT_AMBIGUOUS = "parent_ambiguous";
	private static final String JSON_BAD_TREE = "bad_tree";
	private static final String CONF_URI_HOST_PATTERN = "org.wikimedia.west1.traces.uriHostPattern";
	private static final String CONF_KEEP_AMBIGUOUS_TREES = "org.wikimedia.west1.traces.keepAmbiguousTrees";
	private static final String CONF_KEEP_BAD_TREES = "org.wikimedia.west1.traces.keepBadTrees";
	private static final Pattern WIKI_HOST_PATTERN = Pattern.compile("[a-z]+://[^/]*wiki.*");

	private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(
	// "x_analytics",
	// "range",
	// "accept_language",
	// "x_forwarded_for",
	// "cache_status",
	// "hostname",
	// "response_size",
	// "uri_host",
	// "ip",
	// "http_method",
	// "time_firstbyte",
	// "sequence",
	// "user_agent",
	    "content_type", "dt", "uri_path", "uri_query", "http_status", "referer",
	    // The fields we added.
	    JSON_CHILDREN, JSON_PARENT_AMBIGUOUS, JSON_BAD_TREE));

	private Pattern mainPagePattern;
	private boolean keepAmbiguousTrees;
	private boolean keepBadTrees;

	private static Text makeTreeId(Text dayAndUid, int seqNum) {
		String[] day_uid = dayAndUid.toString().split(GroupAndFilterMapper.UID_SEPARATOR, 2);
		String day = day_uid[0];
		String uidHash = DigestUtils.md5Hex(day_uid[1]);
		// seqNum is zero-padded to fixed length 4: we allow at most MAX_NUM_PAGEVIEWS = 3600 pageviews
		// per day, and in the worst case, each pageview is its own tree, so seqNum <= 3600.
		return new Text(String.format("%s_%s_%04d", day, uidHash, seqNum));
	}

	private static void sparsifyJson(JSONObject json) {
		// First process children recursively.
		if (json.has(JSON_CHILDREN)) {
			JSONArray children = json.getJSONArray(JSON_CHILDREN);
			for (int i = 0; i < children.length(); ++i) {
				sparsifyJson((JSONObject) children.get(i));
			}
		}
		// Then process the root itself.
		for (String field : JSONObject.getNames(json)) {
			if (!FIELDS_TO_KEEP.contains(field)) {
				json.remove(field);
			}
		}
	}

	protected boolean isGoodPageview(JSONObject root, boolean isGlobalRoot) throws JSONException {
		return
		// No node can be from the last hour of the day, so we discard sessions spanning the day
		// boundary.
		!root.getString("dt").contains("T23:")
		// The root must not be a Wikimedia page; this will discard traces that in fact continue a
		// previous session (after a break of more than INTER_SESSION_TIME, or because it's the
		// second part of a session that starts the day boundary and whose first part was excluded
		// via the "T23:" rule.
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

	private List<Pageview> filterTrees(List<Pageview> roots) {
		List<Pageview> filtered = new ArrayList<Pageview>();
		for (Pageview root : roots) {
			try {
				if (isGoodTree(root.json, true)) {
					sparsifyJson(root.json);
					filtered.add(root);
				} else if (keepBadTrees) {
					sparsifyJson(root.json);
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
		List<Pageview> roots = new ArrayList<Pageview>();
		Map<String, Integer> urlCounts = new HashMap<String, Integer>();
		Map<String, Pageview> urlToLastPageview = new HashMap<String, Pageview>();
		// Iterate over all pageviews in temporal order.
		for (Pageview pv : session) {
			Pageview parent = urlToLastPageview.get(pv.referer);
			if (parent == null) {
				roots.add(pv);
				pv.json.put(JSON_PARENT_AMBIGUOUS, false);
			} else {
				parent.json.append(JSON_CHILDREN, pv.json);
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

	public List<Pageview> sequenceToTrees(List<Pageview> pageviews) throws JSONException,
	    ParseException {
		// Sort all pageviews by time; this sort is stable, so requests that have the same timestamp
		// will stay in the original order.
		Collections.sort(pageviews, new Comparator<Pageview>() {
			@Override
			public int compare(Pageview pv1, Pageview pv2) {
				return (int) (pv1.time - pv2.time);
			}
		});

		// Return the list of valid trees.
		return filterTrees(getMinimumSpanningForest(pageviews));
	}

	private void setDefaultConfig() {
		mainPagePattern = Pattern.compile("http.?://pt\\.wikipedia\\.org/");
		keepAmbiguousTrees = true;
		keepBadTrees = true;
	}

	@Override
	public void configure(JobConf conf) {
		if (conf == null) {
			setDefaultConfig();
		} else {
			mainPagePattern = Pattern.compile("http.?://(" + conf.get(CONF_URI_HOST_PATTERN, "") + ")/");
			keepAmbiguousTrees = conf.getBoolean(CONF_KEEP_AMBIGUOUS_TREES, true);
			keepBadTrees = conf.getBoolean(CONF_KEEP_BAD_TREES, false);
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
			List<Pageview> roots = sequenceToTrees(pageviews);
			int i = 0;
			for (Pageview root : roots) {
				out.collect(makeTreeId(dayAndUid, i), new Text(root.toString()));
				++i;
			}
		} catch (Exception e) {
			System.out.format("%s\n", e.getMessage());
		}
	}

}
