package org.wikimedia.west1.traces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractor {

	public static List<Pageview> makeTestSession() throws Exception {
		List<Pageview> session = new ArrayList<Pageview>();
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":1,\"dt\":\"2014-12-04T01:00:10\",\"uri_path\":\"a\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"-\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":2,\"dt\":\"2014-12-04T01:00:15\",\"uri_path\":\"c\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":3,\"dt\":\"2014-12-04T01:00:20\",\"uri_path\":\"b\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":4,\"dt\":\"2014-12-04T01:00:25\",\"uri_path\":\"a\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"b\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":5,\"dt\":\"2014-12-04T01:00:30\",\"uri_path\":\"b\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"a\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":6,\"dt\":\"2014-12-04T01:00:40\",\"uri_path\":\"c\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"b\"}")));
		session
		    .add(new Pageview(
		        new JSONObject(
		            "{\"sequence\":7,\"dt\":\"2014-12-04T01:00:50\",\"uri_path\":\"d\",\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"c\"}")));
		return session;
	}

	private void test() throws Exception {
		List<Pageview> session = makeTestSession();
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
