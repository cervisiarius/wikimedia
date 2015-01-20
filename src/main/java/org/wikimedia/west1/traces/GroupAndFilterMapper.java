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

import ua_parser.Parser;

public class GroupAndFilterMapper implements Mapper<Text, Text, Text, Text> {

	public static final String UID_SEPARATOR = "###";
	private static final String CONF_URI_HOST_PATTERN = "org.wikimedia.west1.traces.uriHostPattern";
	private static final Pattern WIKI_PATTERN = Pattern.compile("/wiki/.*");
	private static final String JSON_IP = "ip";
	private static final String JSON_DATETIME = "dt";
	private static final String JSON_USERAGENT = "user_agent";
	private static final String JSON_XFF = "x_forwarded_for";
	private static final String JSON_URIPATH = "uri_path";
	private static final String JSON_URIHOST = "uri_host";

	// A regex of the Wikimedia sites we're interested in, e.g., "(pt|es)\\.wikipedia\\.org".
	private Pattern uriHostPattern;
	// A parser for determining whether requests come from a bot.
	private Parser uaParser;

	@Override
	public void configure(JobConf conf) {
		uriHostPattern = Pattern.compile(conf.get(CONF_URI_HOST_PATTERN, ".*"));
		try {
			uaParser = new Parser();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
	}

	// Extract the last IP address from the x_forwarded_for string.
	private static String processXForwardedFor(String xff) {
		return xff.substring(xff.lastIndexOf(", ") + 2);
	}

	private static String extractDayFromDate(String date) {
		return date.substring(0, date.indexOf('T'));
	}
	
	private boolean isBot(String userAgent) {
		return uaParser.parseDevice(userAgent).family.equals("Spider");
	}

	// We want to send everything the same user did on the same day to the same reducer.
	// Users are represented as the tripe (ip, x_forwarded_for, user_agent).
	protected static String makeKey(JSONObject json) throws JSONException {
		String ip = json.getString(JSON_IP);
		String ua = json.getString(JSON_USERAGENT);
		String xff = processXForwardedFor(json.getString(JSON_XFF));
		String day = extractDayFromDate(json.getString(JSON_DATETIME));
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s%s%s%s%s%s%s", day, UID_SEPARATOR, ip, UID_SEPARATOR, xff,
		    UID_SEPARATOR, ua).replace('\t', ' ');
	}

	@Override
	public void map(Text key, Text jsonString, OutputCollector<Text, Text> out, Reporter reporter)
	    throws IOException {
		try {
			JSONObject json = new JSONObject(jsonString.toString());
			if (// The request must be for one of the whitelisted Wikimedia sites.
					uriHostPattern.matcher(json.getString(JSON_URIHOST)).matches()
					// It must be to an article page, i.e., the path must start with "/wiki/".
			    && WIKI_PATTERN.matcher(json.getString(JSON_URIPATH)).matches()
			    // It can't be from a bot.
			    && isBot(json.getString(JSON_USERAGENT))) {
				out.collect(new Text(makeKey(json)), jsonString);
			}
		} catch (JSONException e) {
			System.out.format("%s\n", e.getMessage());
		}
	}

}
