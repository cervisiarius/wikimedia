package org.wikimedia.west1.traces;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONException;
import org.json.JSONObject;

public class GroupAndFilterMapper implements Mapper<Text, Text, Text, Text> {

	public static final String UID_SEPARATOR = "###";
	private static final String CONF_URI_HOST_PATTERN = "org.wikimedia.west1.traces.uriHostPattern";
	private static final Pattern WIKI_PATTERN = Pattern.compile("/wiki/.*");

	private Pattern uriHostPattern;

	@Override
	public void configure(JobConf conf) {
		uriHostPattern = Pattern.compile(conf.get(CONF_URI_HOST_PATTERN, ".*"));
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

	protected static String makeKey(JSONObject json) throws JSONException {
		String ip = json.getString("ip");
		String ua = json.getString("user_agent");
		String xff = processXForwardedFor(json.getString("x_forwarded_for"));
		String day = extractDayFromDate(json.getString("dt"));
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s%s%s%s%s%s%s", day, UID_SEPARATOR, ip, UID_SEPARATOR, xff,
		    UID_SEPARATOR, ua).replace('\t', ' ');
	}

	@Override
	public void map(Text key, Text jsonString, OutputCollector<Text, Text> out, Reporter reporter)
	    throws IOException {
		try {
			JSONObject json = new JSONObject(jsonString.toString());
			if (uriHostPattern.matcher(json.getString("uri_host")).matches()
			    && WIKI_PATTERN.matcher(json.getString("uri_path")).matches()) {
				out.collect(new Text(makeKey(json)), jsonString);
			}
		} catch (JSONException e) {
			System.out.format("%s\n", e.getMessage());
		}
	}

}