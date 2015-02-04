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
	private static final String CONF_LANGUAGE_PATTERN = "org.wikimedia.west1.traces.languagePattern";
	private static final String JSON_IP = "ip";
	private static final String JSON_DT = "dt";
	private static final String JSON_UA = "user_agent";
	private static final String JSON_XFF = "x_forwarded_for";
	private static final String JSON_URI_PATH = "uri_path";
	private static final String JSON_URI_HOST = "uri_host";
	private static final String JSON_HTTP_STATUS = "http_status";
	private static final String JSON_ACCEPT_LANG = "accept_language";

	// NB: Special pages can have different names in different languages.
	private static final Pattern SPECIAL_NAMESPACE_PATTERN = Pattern
	    .compile("(?i)/wiki/("
	        + "Arbennek|Arbennig|Arnawl\u0131|Astamiwa|Ba\u011fse|Berezi|Dibar|Er_lheh|Erenoam\u00e1\u0161|Eri|Especial|Espesi\u00e1l|Espesial|Espesiat|Espesyal|Extra|Husus|Ih\u00fc_k\u00e1r\u00edr\u00ed|Immikkut|Ispetziale|Istimewa|Istimiwa|Jagleel|Kerfiss\u00ed\u00f0a|Khas|Kusuih|Maalum|Maasus|Mahsus|Manokana|Maxsus|Mba'ech\u0129ch\u0129|N\u014dncuahqu\u012bzqui|Natatangi|P\u00e0t\u00e0k\u00ec|Papa_nui|Patikos|Pinaurog|Posebno|S\u00f2nraichte|Sapaq|Schbezial|Serstakt|Sevi\u0161kuo|Sipeci\u00e5s|Sipesol|Soronko|Sp\u00e8ci\u00e2l|Sp\u00e9cial|Spe\u00e7iale|Spe\u010bjali|Spec\u0113los|Specala|Speci\u00e0le|Speci\u00e1lis|Speci\u00e1ln\u00ed|Speci\u00e2l|Speciaal|Special|Speciala\u0135o|Speciale|Specialine|Specialis|Specialne|Specialnje|Specialus|Speciaol|Speciel|Specioal|Specjaln\u00f4|Specjalna|Speisialta|Spesiaal|Spesial|Spesyal|Spezial|Spiciali|Syndrig|Szpecyjalna|Tallituslehek\u00fclg|Taybet|Tek-pia\u030dt|Toiminnot|Uslig|Uzalutno|Wiki|X\u00fcsusi|\u00d6zel|\u00dd\u00f6rite|\u0110\u1eb7c_bi\u1ec7t|\u0160peci\u00e1lne|\u0395\u03b9\u03b4\u03b9\u03ba\u03cc|\u0395\u03b9\u03b4\u03b9\u03ba\u03cc\u03bd|\u0410\u0434\u043c\u044b\u0441\u043b\u043e\u0432\u0430\u0435|\u0410\u043d\u0430\u043b\u043b\u0430\u0430\u0445|\u0410\u0440\u043d\u0430\u0439\u044b|\u0410\u0442\u0430\u0439\u044b\u043d|\u0411\u0430\u0448\u043a\u0430|\u0411\u0430\u0448\u043a\u0430_\u0442\u0435\u0432\u0435\u043d\u044c|\u0411\u0435\u043b\u0445\u0430\u043d|\u0412\u0438\u0436\u0430|\u041a\u044a\u0443\u043b\u043b\u0443\u0433\u044a\u0438\u0440\u0430\u043b_\u043b\u0430\u0436\u0438\u043d|\u041a\u044a\u0443\u043b\u043b\u0443\u043a\u044a|\u041a\u04e9\u0434\u043b\u0445\u043d\u04d9|\u041b\u04f1\u043c\u044b\u043d_\u044b\u0448\u0442\u044b\u043c\u0435|\u041c\u0430\u0445\u0441\u0443\u0441|\u041d\u0430\u0440\u043e\u0447\u044c\u043d\u0430|\u041e\u0442\u0441\u0430\u0441\u044f\u043d|\u041f\u0430\u043d\u0435\u043b\u044c|\u041f\u043e\u0441\u0435\u0431\u043d\u043e|\u0421\u00e6\u0440\u043c\u0430\u0433\u043e\u043d\u0434|\u0421\u043b\u0443\u0436\u0435\u0431\u043d\u0430\u044f|\u0421\u043f\u0435\u0446\u0438\u0430\u043b\u043d\u0438|\u0421\u043f\u0435\u0446\u0438\u0458\u0430\u043b\u043d\u0430|\u0421\u043f\u0435\u0446\u0456\u0430\u043b\u044c\u043d\u0430|\u0421\u043f\u0435\u0446\u04f9\u043b\u04f9\u0448\u0442\u04d3\u0448|\u0421\u043f\u044d\u0446\u044b\u044f\u043b\u044c\u043d\u044b\u044f|\u0422\u0443\u0441\u0433\u0430\u0439|\u0422\u0443\u0441\u043a\u0430\u0439|\u0422\u0443\u0441\u0445\u0430\u0439|\u0426\u0430\u0441\u0442\u04d9\u0438|\u0428\u043f\u0435\u0446\u0456\u0430\u043b\u043d\u0430|\u042f\u0442\u0430\u0440\u043b\u0103|\u054d\u057a\u0561\u057d\u0561\u0580\u056f\u0578\u0572|\u05d1\u05d0\u05b7\u05d6\u05d5\u05e0\u05d3\u05e2\u05e8|\u05de\u05d9\u05d5\u05d7\u05d3|\u0626\u0627\u0644\u0627\u06be\u0649\u062f\u06d5|\u062a\u0627\u06cc\u0628\u06d5\u062a|\u062e\u0627\u0635|\u0634\u0627|\u0648\u06cc\u0698\u0647|\u0681\u0627\u0646\u06ab\u0693\u06cc|\u0715\u071d\u0720\u0722\u071d\u0710|\u079a\u07a7\u0787\u07b0\u0790\u07a6|\u0935\u093f\u0936\u0947\u0937|\u0935\u093f\u0936\u0947\u0937\u092e\u094d|\u0935\u093f\u0938\u0947\u0938|\u09ac\u09bf\u09b6\u09c7\u09b7|\u0a16\u0a3c\u0a3e\u0a38|\u0ab5\u0abf\u0ab6\u0ac7\u0ab7|\u0b2c\u0b3f\u0b36\u0b47\u0b37|\u0b9a\u0bbf\u0bb1\u0baa\u0bcd\u0baa\u0bc1|\u0c2a\u0c4d\u0c30\u0c24\u0c4d\u0c2f\u0c47\u0c15|\u0cb5\u0cbf\u0cb6\u0cc7\u0cb7|\u0d2a\u0d4d\u0d30\u0d24\u0d4d\u0d2f\u0d47\u0d15\u0d02|\u0dc0\u0dd2\u0dc1\u0dda\u0dc2|\u0e1e\u0e34\u0e40\u0e28\u0e29|\u0e9e\u0eb4\u0ec0\u0eaa\u0e94|\u10e1\u10de\u10d4\u10ea\u10d8\u10d0\u10da\u10e3\u10e0\u10d8|\u120d\u12e9|\u1796\u17b7\u179f\u17c1\u179f|\u7279\u5225|\u7279\u6b8a|\ud2b9\uc218|khaas"
	        + "):.*");

	private static enum HADOOP_COUNTERS {
		SKIPPED_BAD_HOST, SKIPPED_BAD_PATH, SKIPPED_SPECIAL_PAGE, SKIPPED_BOT, SKIPPED_BAD_HTTP_STATUS, OK_REQUEST, MAP_EXCEPTION
	}

	// A regex of the Wikimedia sites we're interested in, e.g., "(pt|es)\\.wikipedia\\.org".
	// This is set via the job config.
	private Pattern uriHostPattern;
	// A parser for determining whether requests come from a bot.
	private Parser uaParser;

	@Override
	public void configure(JobConf conf) {
		uriHostPattern = Pattern.compile(String.format("(%s)\\.wikipedia\\.org",
		    conf.get(CONF_LANGUAGE_PATTERN, "pt")));
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
		String acceptLang = json.getString(JSON_ACCEPT_LANG);
		String xff = processXForwardedFor(json.getString(JSON_XFF));
		String day = extractDayFromDate(json.getString(JSON_DT));
		String uriHost = json.getString(JSON_URI_HOST);
		String lang = uriHost.substring(0, uriHost.indexOf('.'));
		// If x_forwarded_for contains an IP address, use that address, otherwise use the address from
		// from the ip field.
		String ipForKey = (xff == null) ? ip : xff;
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s%s%s%s%s%s%s%s%s", lang, UID_SEPARATOR, day, UID_SEPARATOR, ipForKey,
		    UID_SEPARATOR, ua, UID_SEPARATOR, acceptLang).replace('\t', ' ');
	}

	@Override
	public void map(Text jsonString, Text emptyValue, OutputCollector<Text, Text> out,
	    Reporter reporter) throws IOException {
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
			else if (SPECIAL_NAMESPACE_PATTERN.matcher(json.getString(JSON_URI_PATH)).matches()) {
				reporter.incrCounter(HADOOP_COUNTERS.SKIPPED_SPECIAL_PAGE, 1);
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
