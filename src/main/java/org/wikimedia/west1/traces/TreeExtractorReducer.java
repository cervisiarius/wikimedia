package org.wikimedia.west1.traces;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractorReducer implements Reducer<Text, Text, Text, Text> {

	// Having 3600 pageviews in a day would mean one every 24 seconds, a lot...
	private static final int MAX_NUM_PAGEVIEWS = 3600;
	// If we see this much time between pageviews, we start a new session; we use one hour.
	private static final long INTER_SESSION_TIME = 3600 * 1000;

	private static Text makeSessionId(Text uidAndDay) {
		return new Text(DigestUtils.md5Hex(uidAndDay.toString()));
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
			String url = pv.url;
			String referer = pv.json.getString("referer");
			Pageview parent = urlToLastPageview.get(referer);
			if (parent == null) {
				roots.add(pv);
				pv.json.put("parent_ambiguous", false);
			} else {
				parent.json.append("z_children", pv.json);
				pv.json.put("parent_ambiguous", urlCounts.get(referer) > 1);
			}
			// Remember this pageview as the last pageview for its URL.
			urlToLastPageview.put(url, pv);
			// Update the counter for this URL.
			Integer c = urlCounts.get(url);
			urlCounts.put(url, c == null ? 1 : c + 1);
		}
		return roots;
	}

	private List<Pageview> sequenceToTrees(List<Pageview> pageviews) throws JSONException,
	    ParseException {
		// Sort all pageviews by time; this sort is stable, so requests that have the same timestamp
		// will stay in the original order.
		Collections.sort(pageviews, new Comparator<Pageview>() {
			@Override
			public int compare(Pageview pv1, Pageview pv2) {
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
				roots.addAll(getMinimumSpanningForest(session));
				session.clear();
			}
			prevTime = curTime;
			session.add(pv);
		}
		// Store the last set of trees.
		roots.addAll(getMinimumSpanningForest(session));

		return roots;
	}

	@Override
	public void configure(JobConf conf) {
	}

	@Override
	public void close() throws IOException {
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
				out.collect(makeSessionId(uidAndDay), new Text(root.toString()));
			}
		} catch (Exception e) {
			System.out.format("%s\n", e.getMessage());
		}
	}

	private void test() throws Exception {
		List<Pageview> session = new ArrayList<Pageview>();
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":1,\"dt\":\"2014-12-04T01:00:10\",\"uri_path\":\"a\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"-\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":2,\"dt\":\"2014-12-04T01:00:15\",\"uri_path\":\"c\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":3,\"dt\":\"2014-12-04T01:00:20\",\"uri_path\":\"b\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":4,\"dt\":\"2014-12-04T01:00:25\",\"uri_path\":\"a\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://b\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":5,\"dt\":\"2014-12-04T01:00:30\",\"uri_path\":\"b\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":6,\"dt\":\"2014-12-04T01:00:40\",\"uri_path\":\"c\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://b\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":7,\"dt\":\"2014-12-04T01:00:50\",\"uri_path\":\"d\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"http://c\"}")));
		for (Pageview root : sequenceToTrees(session)) {
			System.out.println(root.toString(2));
		}
	}

	public static void main(String[] args) throws Exception {
		String pvString = "{\"hostname\":\"cp1066.eqiad.wmnet\",\"sequence\":1470486742,"
		    + "\"dt\":\"2014-12-04T01:00:00\",\"time_firstbyte\":0.000128984,\"ip\":\"0.0.0.0\","
		    + "\"cache_status\":\"hit\",\"http_status\":\"200\",\"response_size\":3185,"
		    + "\"http_method\":\"GET\",\"uri_host\":\"en.wikipedia.org\",\"uri_path\":\"/w/index.php\","
		    + "\"uri_query\":\"?title=MediaWiki:Gadget-refToolbarBase.js&action=raw&ctype=text/javascript\","
		    + "\"content_type\":\"text/javascript; charset=UTF-8\","
		    + "\"referer\":\"http://es.wikipedia.org/wiki/Jos%C3%A9_Mar%C3%ADa_Yazpik\","
		    + "\"x_forwarded_for\":\"-\","
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\","
		    + "\"accept_language\":\"es-MX\",\"x_analytics\":\"php=hhvm\",\"range\":\"-\"}";
		TreeExtractorReducer reducer = new TreeExtractorReducer();
		reducer.test();
	}

}
