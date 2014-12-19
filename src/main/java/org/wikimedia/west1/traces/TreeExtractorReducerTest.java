package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractorReducerTest {

	private static TreeExtractorReducer reducer = new TreeExtractorReducer();

	static {
		reducer.configure(null);
	}

	private static Pageview makePageview(int seqNum, int time, String url, String referer)
	    throws JSONException, ParseException {
		String format = "{\"sequence\":%d,\"dt\":\"2014-12-04T01:00:%02d\",\"uri_path\":\"%s\","
		    + "\"uri_host\":\"\",\"uri_query\":\"\",\"referer\":\"%s\"}";
		return new Pageview(new JSONObject(String.format(format, seqNum, time, url, referer)));
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
		for (Pageview root : reducer.sequenceToTrees(session)) {
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
		System.out.println(reducer.isGoodPageview(json, false, true));
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
		System.out.println(reducer.isGoodTree(json, true));
	}

	public static void testPruneBadLeaves() throws Exception {
		//String pvString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:23:54\",\"uri_path\":\"/wiki/Cl%C3%A1udio_Manuel_da_Costa\",\"range\":\"-\",\"accept_language\":\"pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4\",\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"children\":[{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:24:01\",\"uri_path\":\"/w/index.php\",\"range\":\"-\",\"accept_language\":\"pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4\",\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp1068.eqiad.wmnet\",\"response_size\":0,\"uri_query\":\"?title=MediaWiki:Gadget-featured-articles-links.js&action=raw&ctype=text/javascript\",\"uri_host\":\"en.wikipedia.org\",\"ip\":\"179.185.83.66\",\"http_method\":\"GET\",\"http_status\":\"304\",\"time_firstbyte\":1.72853E-4,\"sequence\":1468582733,\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36\",\"referer\":\"http://pt.wikipedia.org/wiki/Cl%C3%A1udio_Manuel_da_Costa\",\"content_type\":\"text/javascript; charset=UTF-8\",\"parent_ambiguous\":false}],\"hostname\":\"cp1068.eqiad.wmnet\",\"response_size\":27918,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\",\"ip\":\"179.185.83.66\",\"http_method\":\"GET\",\"http_status\":\"200\",\"time_firstbyte\":1.84536E-4,\"sequence\":1468572810,\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36\",\"referer\":\"https://www.google.com.br/\",\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		String pvString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:23:54\",\"uri_path\":\"/wiki/Cl%C3%A1udio_Manuel_da_Costa\",\"range\":\"-\",\"accept_language\":\"pt-BR,pt;q=0.8,en-US;q=0.6,en;q=0.4\",\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\","
				+ "\"hostname\":\"cp1068.eqiad.wmnet\",\"response_size\":27918,\"uri_query\":\"\",\"uri_host\":\"pt.wikipedia.org\",\"ip\":\"179.185.83.66\",\"http_method\":\"GET\",\"http_status\":\"200\",\"time_firstbyte\":1.84536E-4,\"sequence\":1468572810,\"user_agent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36\",\"referer\":\"https://www.google.com.br/\",\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		JSONObject json = new JSONObject(pvString);
		System.out.println(json.toString(2));
		System.out.println(reducer.pruneBadLeaves(json, true).toString(2));
	}

	public static void main(String[] args) throws Exception {
		testPruneBadLeaves();
	}

}
