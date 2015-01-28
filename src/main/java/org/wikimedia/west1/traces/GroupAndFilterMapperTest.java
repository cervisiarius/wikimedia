package org.wikimedia.west1.traces;

import java.util.regex.Pattern;

import org.json.JSONObject;

import ua_parser.Parser;

public class GroupAndFilterMapperTest {

	private static void testUAParser() throws Exception {
		Parser p = new Parser();
		System.out.println(p.parseDevice("Googlebot-Image/1.0").family);
	}

	private static void testNonArticlePagePattern() {
		final Pattern NON_ARTICLE_PAGE_PATTERN = Pattern
		    .compile("(?i)/wiki/(Image|Media|Special|Especial|Spezial|Talk|User|Wikipedia|File"
		        + "|MediaWiki|Template|Help|Book|Draft|Education[_ ]Program|TimedText|Module|Wikt)"
		        + "([_ ]talk)?:.*");
		System.out.println(NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/file:test").matches());
		System.out.println(NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/User talk:test").matches());
		System.out.println(NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/User_talk:test").matches());
		System.out.println(NON_ARTICLE_PAGE_PATTERN.matcher("/wiki/test").matches());
	}

	private static void testMakeKey() throws Exception {
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
		System.out.println(GroupAndFilterMapper.makeKey(new JSONObject(pvString)));
	}

	public static void main(String[] args) throws Exception {
		testNonArticlePagePattern();
	}

}
