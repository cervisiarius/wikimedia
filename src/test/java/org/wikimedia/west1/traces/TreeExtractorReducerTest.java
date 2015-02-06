package org.wikimedia.west1.traces;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractorReducerTest {

	private static TreeExtractorReducer reducer = new TreeExtractorReducer();

	static {
		reducer.configure(null);
	}

	private static Map<String, List<String>> readReverseRedirectsFromFile(String file) throws IOException {
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		InputStream is = new FileInputStream(new File(file));
		if (file.endsWith(".gz")) {
			try {
				is = new GZIPInputStream(is);
			} catch (IOException e) {
			}
		}
		Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
		while (sc.hasNext()) {
			String[] tokens = sc.next().split("\t", 2);
			String src = tokens[0];
			String tgt = tokens[1];
			List<String> srcForTgt = map.get(tgt);
			if (srcForTgt == null) {
				srcForTgt = new ArrayList<String>();
				map.put(tgt, srcForTgt);
			}
			srcForTgt.add(src);
		}
		sc.close();
		return map;
	}

	private static Pageview makePageview(int seqNum, int time, String url, String referer)
	    throws JSONException, ParseException {
		String format = "{\"sequence\":%d,\"dt\":\"2014-12-04T01:00:%02d\",\"uri_path\":\"%s\","
		    + "\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"%s\"}";
		return new Pageview(new JSONObject(String.format(format, seqNum, time, url, referer)), null);
	}

	public static void testSequenceToTrees() throws Exception {
		List<Pageview> session = new ArrayList<Pageview>();
		session.add(makePageview(1, 10, "a", "-"));
		session.add(makePageview(2, 15, "c", "http://a"));
		session.add(makePageview(3, 20, "b", "http://a"));
		session.add(makePageview(4, 25, "a", "http://b"));
		session.add(makePageview(5, 30, "b", "http://a"));
		session.add(makePageview(6, 40, "c", "http://b"));
		session.add(makePageview(7, 50, "d", "http://c"));
		for (Pageview root : reducer.sequenceToTrees(session, null)) {
			System.out.println(root.toString(2));
		}
	}

	public static void testIsGoodPageview() throws Exception {
		String pvString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:18:34\","
		    + "\"uri_path\":\"/wiki/Indonesia\",\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4016.ulsfo.wmnet\","
		    + "\"response_size\":90662,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.26839E-4,\"sequence\":1261697215,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
		    + "\"children\":[{\"x_analytics\":\"php=zend\",\"dt\":\"2014-12-04T01:19:14\","
		    + "\"uri_path\":\"/wiki/Bali\",\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4018.ulsfo.wmnet\","
		    + "\"response_size\":60137,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.32561E-4,\"sequence\":1263338019,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
		    + "\"referer\":\"http://pt.wikipedia.org/wiki/Indonesia\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}],"
		    + "\"referer\":\"http://pt.wikipedia.org/wiki/Teochew_dialect\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		JSONObject json = new JSONObject(pvString);
		System.out.println(json.toString(2));
		System.out.println(reducer.isGoodPageview(json, false, null, null));
	}

	public static void testIsGoodTree() throws Exception {
		String pvString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:18:34\","
		    + "\"uri_path\":\"/wiki/Indonesia\",\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4016.ulsfo.wmnet\","
		    + "\"response_size\":90662,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.26839E-4,\"sequence\":1261697215,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
		    + "\"children\":[{\"x_analytics\":\"php=zend\",\"dt\":\"2014-12-04T01:19:14\","
		    + "\"uri_path\":\"/wiki/Bali\",\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4018.ulsfo.wmnet\","
		    + "\"response_size\":60137,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.32561E-4,\"sequence\":1263338019,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
		    + "\"referer\":\"http://pt.wikipedia.org/wiki/Indonesia\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}],"
		    + "\"referer\":\"http://pt.wikipedia.org/wiki/Teochew_dialect\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		JSONObject json = new JSONObject(pvString);
		System.out.println(json.toString(2));
		System.out.println(reducer.isGoodTree(json, 0, null, null));
	}

	public static void testPageview() throws Exception {
		Map<String, List<String>> revRdirects = readReverseRedirectsFromFile(System.getenv("HOME")
		    + "/wikimedia/trunk/data/ptwiki_20141104_redirects.tsv.gz");
		String pvString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:18:34\","
		    + "\"uri_path\":\"/wiki/Antraz\",\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4016.ulsfo.wmnet\","
		    + "\"response_size\":90662,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.26839E-4,\"sequence\":1261697215,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
		    + "\"referer\":\"http://pt.wikipedia.org/wiki/Caf%C3%A9-do-bugre\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		Pageview pv = new Pageview(new JSONObject(pvString), null);
		System.out.println(pv.toString(2));
		System.out.println(pv.resolvedArticle);
		System.out.println(pv.refererArticle);
		System.out.println(revRdirects.get("Macaxeira"));
	}

	public static void main(String[] args) throws Exception {
		//System.out.println(GroupAndFilterMapper.NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/Wikipédia:Página_principal").matches());
		testPageview();
	}

}
