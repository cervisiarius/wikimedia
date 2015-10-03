package org.wikimedia.west1.hoaxes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import parquet.example.data.Group;
import ua_parser.Parser;

public class HoaxLogExtractorMapper extends Mapper<LongWritable, Group, Text, Text> {

	// JSON field names.
	private static final String JSON_IP = "ip";
	private static final String JSON_DT = "dt";
	private static final String JSON_UA = "user_agent";
	private static final String JSON_HTTP_STATUS = "http_status";
	private static final String JSON_REFERER = "referer";
	private static final String JSON_URI_PATH = "uri_path";
	private static final String JSON_URI_HOST = "uri_host";
	private static final String JSON_ACCEPT_LANG = "accept_language";
	private static final String JSON_XFF = "x_forwarded_for";

	// An ad-hoc pattern to catch bots beyond those captured by the ua_parser library.
	private static final Pattern FORBIDDEN_UA_PATTERN = Pattern
	    .compile(".*([Bb]ot|[Ss]pider|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks).*|Java/1\\..*|curl.*|PHP/.*|-|");

	public static final String[] INPUT_FIELDS = { JSON_DT, JSON_IP, JSON_HTTP_STATUS, JSON_URI_HOST,
	    JSON_URI_PATH, JSON_REFERER, JSON_XFF, JSON_UA, JSON_ACCEPT_LANG };

	private static enum HADOOP_COUNTERS {
		MAP_SKIPPED_BAD_TITLE, MAP_SKIPPED_BAD_HOST, MAP_SKIPPED_BAD_PATH, MAP_SKIPPED_SPECIAL_PAGE, MAP_SKIPPED_BOT, MAP_SKIPPED_BAD_HTTP_STATUS, MAP_OK_REQUEST, MAP_EXCEPTION, MAP_GOOD_TITLE_IN_REFERER, MAP_GOOD_TITLE_IN_URL
	}

	// A parser for determining whether requests come from a bot.
	private Parser uaParser;
	private Set<String> titles;

	@Override
	public void setup(Context context) {
		try {
			uaParser = new Parser();
			readTitles();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void readTitles() throws IOException {
		titles = new HashSet<String>();
		InputStream is = ClassLoader.getSystemResourceAsStream("hoax_titles.txt");
		Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
		while (sc.hasNext()) {
			titles.add(sc.next());
		}
		sc.close();
		is.close();
	}

	// We want to send everything the same user did to the same reducer.
	// Users are represented as the triple (ip, x_forwarded_for, user_agent).
	protected static String makeKey(JSONObject json) throws JSONException {
		String ip = json.getString(JSON_IP);
		String ua = json.getString(JSON_UA);
		String acceptLang = json.getString(JSON_ACCEPT_LANG);
		String xff = processXForwardedFor(json.getString(JSON_XFF));
		// If x_forwarded_for contains an IP address, use that address, otherwise use the address from
		// from the ip field.
		String ipForKey = (xff == null) ? ip : xff;
		return DigestUtils.md5Hex(String.format("%s###%s###%s", ipForKey, ua, acceptLang));
	}

	// Extract the last IP address from the x_forwarded_for string. If the result doesn't look like
	// an IP address (because it contains no "."), return null.
	private static String processXForwardedFor(String xff) {
		int lastCommaIdx = xff.lastIndexOf(", ");
		String ip = lastCommaIdx >= 0 ? xff.substring(lastCommaIdx + 2) : xff;
		if (ip.contains(".")) {
			return ip;
		} else {
			return null;
		}
	}

	private boolean isBot(String userAgent) {
		return uaParser.parseDevice(userAgent).family.equals("Spider")
		    || FORBIDDEN_UA_PATTERN.matcher(userAgent).matches();
	}

	private static JSONObject createJson(Group data) {
		JSONObject json = new JSONObject();
		for (String field : INPUT_FIELDS) {
			json.put(field, data.getString(field, 0));
		}
		return json;
	}

  protected static final String decode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (Exception e) {
      return s;
    }
  }

	@Override
	public void map(LongWritable key, Group value, Context context) throws IOException,
	    InterruptedException {
		try {
			JSONObject json = createJson(value);
			// The request must be for enwiki.
			if (!json.getString(JSON_URI_HOST).equals("en.wikipedia.org")) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_HOST).increment(1);
				return;
			}
			// It must be to an article page or a wiki search.
			if (!json.getString(JSON_URI_PATH).startsWith("/wiki/")) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_PATH).increment(1);
				return;
			}
			// It can't be from a bot.
			if (isBot(json.getString(JSON_UA))) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BOT).increment(1);
				return;
			}
			// We only accept HTTP statuses 200 (OK), 304 (Not Modified).
			if (!json.getString(JSON_HTTP_STATUS).equals("200")
			    && !json.getString(JSON_HTTP_STATUS).equals("304")) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_HTTP_STATUS).increment(1);
				return;
			}
			String titleInUrl = decode(json.getString(JSON_URI_PATH).substring(6));
			String titleInReferer = null;
			boolean goodTitleInUrl = titles.contains(titleInUrl);
			boolean goodTitleInReferer = false;
			if (json.getString(JSON_REFERER).startsWith("http://en.wikipedia.org/wiki/")
			    || json.getString(JSON_REFERER).startsWith("https://en.wikipedia.org/wiki/")) {
				String[] refererPrefixAndTitle = json.getString(JSON_REFERER).split("\\/wiki\\/", 2);
				String[] titleAndAnchor = refererPrefixAndTitle[1].split("#");
				titleInReferer = decode(titleAndAnchor[0]);
				goodTitleInReferer = titles.contains(titleInReferer);
			}
			if (!goodTitleInUrl && !goodTitleInReferer) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_TITLE).increment(1);
				return;
			}
			if (goodTitleInUrl) {
				json.put("hoax_in_url", titleInUrl);
				context.getCounter(HADOOP_COUNTERS.MAP_GOOD_TITLE_IN_URL).increment(1);
			}
			if (goodTitleInReferer) {
				json.put("hoax_in_referer", titleInReferer);
				context.getCounter(HADOOP_COUNTERS.MAP_GOOD_TITLE_IN_REFERER).increment(1);
			}
			// Only if all those filters were passed do we output the row.
			context.write(new Text(makeKey(json)), new Text(json.toString()));
			context.getCounter(HADOOP_COUNTERS.MAP_OK_REQUEST).increment(1);
		} catch (JSONException e) {
			context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
