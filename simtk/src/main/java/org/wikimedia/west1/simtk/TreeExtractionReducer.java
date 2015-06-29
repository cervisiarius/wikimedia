package org.wikimedia.west1.simtk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractionReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static final int MAX_NUM_EVENTS = 10000;

	// The fields you want to store for every event.
	private static final Set<String> FIELDS_TO_KEEP = new HashSet<String>(Arrays.asList(
	    BrowserEvent.JSON_DT, BrowserEvent.JSON_URI_PATH, BrowserEvent.JSON_URI_QUERY,
	    BrowserEvent.JSON_HTTP_STATUS,
	    // The fields we added.
	    BrowserEvent.JSON_ANCHOR, BrowserEvent.JSON_CHILDREN, BrowserEvent.JSON_PARENT_AMBIGUOUS));
	// The fields you want to store only for the root (because they're identical for all events in
	// the same tree).
	private static final Set<String> FIELDS_TO_KEEP_IN_ROOT = new HashSet<String>(Arrays.asList(
	    BrowserEvent.JSON_UA, BrowserEvent.JSON_REFERER, BrowserEvent.JSON_TREE_ID));

	private static enum HADOOP_COUNTERS {
		REDUCE_OK_TREE, REDUCE_BAD_TREE, REDUCE_TOO_MANY_EVENTS, REDUCE_EXCEPTION
	}

	// Tree ids consist of the language, a salted hash of the UID, the day, and a sequential number
	// (in order of time), e.g., de_5ca697716da3203201f56d09b41c954d_20150118_0004.
	private Text makeTreeId(String uid, String dt, int seqNum) {
		String uidHash = DigestUtils.md5Hex(uid);
		String day = dt.substring(0, dt.indexOf('T')).replace("-", "");
		// seqNum is zero-padded to fixed length 4: if we allow at most MAX_NUM_EVENTS = 10K events
		// per day, and in the worst case, each event is its own tree, then seqNum <= 9999.
		return new Text(String.format("%s_%s_%04d", uidHash, day, seqNum));
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

	@Override
	public void reduce(Text key, Iterable<Text> eventsIt,
	    Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException {
		try {
			List<BrowserEvent> events = new ArrayList<BrowserEvent>();
			int n = 0;
			String uid = key.toString();

			// Collect all events for this user on this day.
			for (Text eventText : eventsIt) {
				// If there are too many event events, output nothing.
				if (++n > MAX_NUM_EVENTS) {
					context.getCounter(HADOOP_COUNTERS.REDUCE_TOO_MANY_EVENTS).increment(1);
					return;
				} else {
					JSONObject json = new JSONObject(eventText.toString());
					BrowserEvent event = new BrowserEvent(json);
					events.add(event);
				}
			}
			// Extract trees and output them.
			List<BrowserEvent> allRoots = sequenceToTrees(events);
			int i = 0;
			for (BrowserEvent root : allRoots) {
				root.json.put(BrowserEvent.JSON_TREE_ID,
				    makeTreeId(uid, root.json.getString(BrowserEvent.JSON_DT), i));
				sparsifyJson(root.json, true);
				context.write(NullWritable.get(), new Text(root.toString()));
				// If the root's referer is a SimTk page, mark the tree as bad.
				if (root.getRefererPathAndQuery() != null) {
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
