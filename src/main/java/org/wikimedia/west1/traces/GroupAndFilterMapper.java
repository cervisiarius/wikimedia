package org.wikimedia.west1.traces;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
	private static final String JSON_IP = "ip";
	private static final String JSON_DT = "dt";
	private static final String JSON_UA = "user_agent";
	private static final String JSON_XFF = "x_forwarded_for";
	private static final String JSON_URI_PATH = "uri_path";
	private static final String JSON_URI_HOST = "uri_host";
	private static final String JSON_HTTP_STATUS = "http_status";

	// NB: Special pages can have different names in different languages.
	private static final Pattern NON_ARTICLE_PAGE_PATTERN = Pattern
	    .compile("(?i)/wiki/(Image|Media|Special|Talk|User|Wikipedia|File|MediaWiki"
	        + "|Template|Help|Book|Draft|Education[_ ]Program|TimedText|Module|Wikt"
	        // Portuguese-specific.
	        + "|Especial|Discuss(\u00E3|%C3A3)o|Usu(\u00E1|%C3%A1)rio\\(a\\)|Wikip(\u00E9|%C3%A9)dia"
	        + "|Ficheiro|Predefini(\u00E7|%C3A7)(\u00E3|%C3A3)o|Ajuda|Livro|M(\u00F3|%C3B3)dulo)"
	        + "([_ ](talk|Discuss(\u00E3|%C3A3)o))?:.*");

	private static final Pattern MAIN_PAGE_PATTERN = Pattern
	    .compile("(?i)/wiki/(Main_Page|Wikip(\u00E9|%C3%A9)dia:P(\u00E1|%C3%A1)gina_principal)");

	private static enum HADOOP_COUNTERS {
		SKIPPED_BAD_HOST, SKIPPED_BAD_PATH, SKIPPED_NON_ARTICLE_PAGE, SKIPPED_BOT, SKIPPED_BAD_HTTP_STATUS, OK_REQUEST, MAP_EXCEPTION
	}

	// A regex of the Wikimedia sites we're interested in, e.g., "(pt|es)\\.wikipedia\\.org".
	// This is set via the job config.
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

	// Extract the last IP address from the x_forwarded_for string. If the result doesn't look like
	// an IP address (because it contains no "."), return null.
	private static String processXForwardedFor(String xff) {
		String ip = xff.substring(xff.lastIndexOf(", ") + 2);
		if (ip.contains(".")) {
			return ip;
		} else {
			return null;
		}
	}

	private static String extractDayFromDate(String date) {
		return date.substring(0, date.indexOf('T')).replace("-", "");
	}

	private boolean isBot(String userAgent) {
		return uaParser.parseDevice(userAgent).family.equals("Spider");
	}

	// We want to send everything the same user did on the same day to the same reducer.
	// Users are represented as the triple (ip, x_forwarded_for, user_agent).
	protected static String makeKey(JSONObject json) throws JSONException {
		String ip = json.getString(JSON_IP);
		String ua = json.getString(JSON_UA);
		String xff = processXForwardedFor(json.getString(JSON_XFF));
		String day = extractDayFromDate(json.getString(JSON_DT));
		// If x_forwarded_for contains an IP address, use that address, otherwise use the address from
		// from the ip field.
		String ipForKey = (xff == null) ? ip : xff;
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s%s%s%s%s", day, UID_SEPARATOR, ipForKey, UID_SEPARATOR, ua).replace(
		    '\t', ' ');
	}

	@Override
	public void map(Text key, Text jsonString, OutputCollector<Text, Text> out, Reporter reporter)
	    throws IOException {
		try {
			JSONObject json = new JSONObject(jsonString.toString());
			// The request must be for one of the whitelisted Wikimedia sites.
			if (!uriHostPattern.matcher(json.getString(JSON_URI_HOST)).matches()) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_BAD_HOST, 1);
				return;
			}
			// It must be to an article page, i.e., the path must start with "/wiki/".
			else if (!json.getString(JSON_URI_PATH).startsWith("/wiki/")) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_BAD_PATH, 1);
				return;
			}
			// Certain page types such as "Special:" and "User:" pages aren't allowed (but we must make
			// sure we're not also banning the main page).
			else if (NON_ARTICLE_PAGE_PATTERN.matcher(json.getString(JSON_URI_PATH)).matches()
			    && !MAIN_PAGE_PATTERN.matcher(json.getString(JSON_URI_PATH)).matches()) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_NON_ARTICLE_PAGE, 1);
				return;
			}
			// It can't be from a bot.
			else if (isBot(json.getString(JSON_UA))) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_BOT, 1);
				return;
			}
			// We only accept HTTP statuses 200 (OK) and 304 (Not Modified).
			else if (!json.getString(JSON_HTTP_STATUS).equals("200")
			    && !json.getString(JSON_HTTP_STATUS).equals("304")) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_BAD_HTTP_STATUS, 1);
				return;
			}
			// Only if all those filters were passed do we output the row.
			else {
				out.collect(new Text(makeKey(json)), jsonString);
				reporter.incrCounter(HADOOP_COUNTERS.OK_REQUEST, 1);
			}
		} catch (JSONException e) {
			reporter.incrCounter(HADOOP_COUNTERS.MAP_EXCEPTION, 1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
