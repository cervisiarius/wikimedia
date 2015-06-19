package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		return new Pageview(new JSONObject(String.format(format, seqNum, time, url, referer)), null);
	}

	public static void testSequenceToTrees() throws Exception {
		List<BrowserEvent> session = new ArrayList<BrowserEvent>();
		session.add(makePageview(1, 10, "a", "-"));
		session.add(makePageview(2, 15, "c", "http://a"));
		session.add(makePageview(3, 20, "b", "http://a"));
		session.add(makePageview(4, 25, "a", "http://b"));
		session.add(makePageview(5, 30, "b", "http://a"));
		session.add(makePageview(6, 40, "c", "http://b"));
		session.add(makePageview(7, 50, "d", "http://c"));
		for (BrowserEvent root : reducer.sequenceToTrees(session, null)) {
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
		System.out.println(reducer.isGoodEvent(json, false, null, null));
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

	public static void testBrowserEvent() throws Exception {
	  Map<String, String> red = new HashMap<String, String>();
    red.put("Antraz", "Antraz_RED");
    red.put("Café-do-bugre", "Café-do-bugre_RED");
		String evString = "{\"x_analytics\":\"php=hhvm\",\"dt\":\"2014-12-04T01:18:34\","
		    + "\"uri_path\":\"/wiki/Antraz\",\"uri_query\":\"\","
        //+ "\"uri_path\":\"/w/index.php\",\"uri_query\":\"?search=Miss%C3%A9+World+2014&title=Special%3ASearch&go=Go&fulltext=1\","
		    + "\"range\":\"-\",\"accept_language\":\"en-US,en;q=0.5\","
		    + "\"x_forwarded_for\":\"-\",\"cache_status\":\"hit\",\"hostname\":\"cp4016.ulsfo.wmnet\","
		    + "\"response_size\":90662,\"uri_host\":\"pt.wikipedia.org\","
		    + "\"ip\":\"1.10.195.119\",\"http_method\":\"GET\",\"http_status\":\"200\","
		    + "\"time_firstbyte\":1.26839E-4,\"sequence\":1261697215,"
		    + "\"user_agent\":\"Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0\","
        //+ "\"referer\":\"http://pt.wikipedia.org/wiki/Caf%C3%A9-do-bugre\","
        + "\"referer\":\"http://pt.wikipedia.org/w/index.php?search=Miss+World+2014&title=Special%3ASearch&go=Go\","
		    + "\"content_type\":\"text/html; charset=UTF-8\",\"parent_ambiguous\":false}";
		BrowserEvent ev = BrowserEvent.newInstance(new JSONObject(evString), red);
		System.out.println(ev.toString(2));
		System.out.println(ev.getPathAndQuery());
		System.out.println(ev.getRefererPathAndQuery());
		System.out.println(ev.time);
	}
	
	public static void testComparison() {
		List<Long> l = Arrays.asList(Long.MIN_VALUE, 0L, Long.MAX_VALUE, Long.MIN_VALUE, 0L, Long.MAX_VALUE);
		//List<Long> l = Arrays.asList(2L, 400L, 1L, 3L);
    Collections.sort(l, new Comparator<Long>() {
      @Override
      public int compare(Long pv1, Long pv2) {
      	// Faulty because of overflows.
        //return (int) (pv1 - pv2);
        if (pv1 > pv2) return 1;
        else if (pv1 < pv2) return -1;
        else return 0;
      }
    });
    System.out.println(l);
	}

	public static void main(String[] args) throws Exception {
		//System.out.println(GroupAndFilterMapper.NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/Wikipédia:Página_principal").matches());
		//testComparison();
	  testBrowserEvent();
	  //testIsGoodTree();
		System.in.read();
	}

}
