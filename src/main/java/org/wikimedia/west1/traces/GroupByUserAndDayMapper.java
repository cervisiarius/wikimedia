package org.wikimedia.west1.traces;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONException;
import org.json.JSONObject;

public class GroupByUserAndDayMapper implements Mapper<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf conf) {
	  conf.setMapOutputKeyClass(Text.class); 
	  conf.setMapOutputValueClass(Text.class); 
}

	@Override
	public void close() throws IOException {
	}

	private static String processXForwardedFor(String xff) {
		return xff.substring(xff.lastIndexOf(", ") + 2);
	}

	private static String extractDayFromDate(String date) {
		return date.substring(0, date.indexOf('T'));
	}

	private static String makeKey(String jsonString) throws JSONException {
		JSONObject json = new JSONObject(jsonString);
		String ip = json.getString("ip");
		String ua = json.getString("user_agent");
		String xff = processXForwardedFor(json.getString("x_forwarded_for"));
		String day = extractDayFromDate(json.getString("dt"));
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s###%s###%s###%s", day, ip, xff, ua).replace('\t', ' ');
	}

	@Override
	public void map(Text key, Text jsonString, OutputCollector<Text, Text> out, Reporter reporter)
	    throws IOException {
		try {
			out.collect(new Text(makeKey(jsonString.toString())), jsonString);
		} catch (JSONException e) {
			System.out.format("%s\n", e.getMessage());
		}
	}

	private void test() throws Exception {
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
		System.out.println(makeKey(pvString));
	}

	public static void main(String[] args) throws Exception {
		GroupByUserAndDayMapper mapper = new GroupByUserAndDayMapper();
		mapper.test();
	}

}
