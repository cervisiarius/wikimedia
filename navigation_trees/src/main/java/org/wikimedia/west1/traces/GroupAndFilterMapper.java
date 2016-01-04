package org.wikimedia.west1.traces;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import parquet.example.data.Group;
import ua_parser.Parser;

public class GroupAndFilterMapper extends Mapper<LongWritable, Group, Text, Text> {

	public static final String UID_SEPARATOR = "###";
	private static final String CONF_LANGUAGE_PATTERN = "org.wikimedia.west1.traces.languagePattern";

	// NB: Special pages can have different names in different languages.
	private static final Pattern SPECIAL_NAMESPACE_PATTERN = Pattern
	    .compile("(?i)/wiki/("
	        + "Arbennek|Arbennig|Arnawl%C4%B1|Arnawl\u0131|Astamiwa|Ba%C4%9Fse|Ba\u011fse|Berezi|%C3%96zel|%C3%9D%C3%B6rite|%C4%90%E1%BA%B7c_bi%E1%BB%87t|%C5%A0peci%C3%A1lne|%CE%95%CE%B9%CE%B4%CE%B9%CE%BA%CF%8C|%CE%95%CE%B9%CE%B4%CE%B9%CE%BA%CF%8C%CE%BD|%D0%90%D0%B4%D0%BC%D1%8B%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D0%B5|%D0%90%D0%BD%D0%B0%D0%BB%D0%BB%D0%B0%D0%B0%D1%85|%D0%90%D1%80%D0%BD%D0%B0%D0%B9%D1%8B|%D0%90%D1%82%D0%B0%D0%B9%D1%8B%D0%BD|%D0%91%D0%B0%D1%88%D0%BA%D0%B0|%D0%91%D0%B0%D1%88%D0%BA%D0%B0_%D1%82%D0%B5%D0%B2%D0%B5%D0%BD%D1%8C|%D0%91%D0%B5%D0%BB%D1%85%D0%B0%D0%BD|%D0%92%D0%B8%D0%B6%D0%B0|%D0%9A%D1%8A%D1%83%D0%BB%D0%BB%D1%83%D0%B3%D1%8A%D0%B8%D1%80%D0%B0%D0%BB_%D0%BB%D0%B0%D0%B6%D0%B8%D0%BD|%D0%9A%D1%8A%D1%83%D0%BB%D0%BB%D1%83%D0%BA%D1%8A|%D0%9A%D3%A9%D0%B4%D0%BB%D1%85%D0%BD%D3%99|%D0%9B%D3%B1%D0%BC%D1%8B%D0%BD_%D1%8B%D1%88%D1%82%D1%8B%D0%BC%D0%B5|%D0%9C%D0%B0%D1%85%D1%81%D1%83%D1%81|%D0%9D%D0%B0%D1%80%D0%BE%D1%87%D1%8C%D0%BD%D0%B0|%D0%9E%D1%82%D1%81%D0%B0%D1%81%D1%8F%D0%BD|%D0%9F%D0%B0%D0%BD%D0%B5%D0%BB%D1%8C|%D0%9F%D0%BE%D1%81%D0%B5%D0%B1%D0%BD%D0%BE|%D0%A1%C3%A6%D1%80%D0%BC%D0%B0%D0%B3%D0%BE%D0%BD%D0%B4|%D0%A1%D0%BB%D1%83%D0%B6%D0%B5%D0%B1%D0%BD%D0%B0%D1%8F|%D0%A1%D0%BF%D0%B5%D1%86%D0%B8%D0%B0%D0%BB%D0%BD%D0%B8|%D0%A1%D0%BF%D0%B5%D1%86%D0%B8%D1%98%D0%B0%D0%BB%D0%BD%D0%B0|%D0%A1%D0%BF%D0%B5%D1%86%D1%96%D0%B0%D0%BB%D1%8C%D0%BD%D0%B0|%D0%A1%D0%BF%D0%B5%D1%86%D3%B9%D0%BB%D3%B9%D1%88%D1%82%D3%93%D1%88|%D0%A1%D0%BF%D1%8D%D1%86%D1%8B%D1%8F%D0%BB%D1%8C%D0%BD%D1%8B%D1%8F|%D0%A2%D1%83%D1%81%D0%B3%D0%B0%D0%B9|%D0%A2%D1%83%D1%81%D0%BA%D0%B0%D0%B9|%D0%A2%D1%83%D1%81%D1%85%D0%B0%D0%B9|%D0%A6%D0%B0%D1%81%D1%82%D3%99%D0%B8|%D0%A8%D0%BF%D0%B5%D1%86%D1%96%D0%B0%D0%BB%D0%BD%D0%B0|%D0%AF%D1%82%D0%B0%D1%80%D0%BB%C4%83|%D5%8D%D5%BA%D5%A1%D5%BD%D5%A1%D6%80%D5%AF%D5%B8%D5%B2|%D7%91%D7%90%D6%B7%D7%96%D7%95%D7%A0%D7%93%D7%A2%D7%A8|%D7%9E%D7%99%D7%95%D7%97%D7%93|%D8%A6%D8%A7%D9%84%D8%A7%DA%BE%D9%89%D8%AF%DB%95|%D8%AA%D8%A7%DB%8C%D8%A8%DB%95%D8%AA|%D8%AE%D8%A7%D8%B5|%D8%B4%D8%A7|%D9%88%DB%8C%DA%98%D9%87|%DA%81%D8%A7%D9%86%DA%AB%DA%93%DB%8C|%DC%95%DC%9D%DC%A0%DC%A2%DC%9D%DC%90|%DE%9A%DE%A7%DE%87%DE%B0%DE%90%DE%A6|Dibar|%E0%A4%B5%E0%A4%BF%E0%A4%B6%E0%A5%87%E0%A4%B7|%E0%A4%B5%E0%A4%BF%E0%A4%B6%E0%A5%87%E0%A4%B7%E0%A4%AE%E0%A5%8D|%E0%A4%B5%E0%A4%BF%E0%A4%B8%E0%A5%87%E0%A4%B8|%E0%A6%AC%E0%A6%BF%E0%A6%B6%E0%A7%87%E0%A6%B7|%E0%A8%96%E0%A8%BC%E0%A8%BE%E0%A8%B8|%E0%AA%B5%E0%AA%BF%E0%AA%B6%E0%AB%87%E0%AA%B7|%E0%AC%AC%E0%AC%BF%E0%AC%B6%E0%AD%87%E0%AC%B7|%E0%AE%9A%E0%AE%BF%E0%AE%B1%E0%AE%AA%E0%AF%8D%E0%AE%AA%E0%AF%81|%E0%B0%AA%E0%B1%8D%E0%B0%B0%E0%B0%A4%E0%B1%8D%E0%B0%AF%E0%B1%87%E0%B0%95|%E0%B2%B5%E0%B2%BF%E0%B2%B6%E0%B3%87%E0%B2%B7|%E0%B4%AA%E0%B5%8D%E0%B4%B0%E0%B4%A4%E0%B5%8D%E0%B4%AF%E0%B5%87%E0%B4%95%E0%B4%82|%E0%B7%80%E0%B7%92%E0%B7%81%E0%B7%9A%E0%B7%82|%E0%B8%9E%E0%B8%B4%E0%B9%80%E0%B8%A8%E0%B8%A9|%E0%BA%9E%E0%BA%B4%E0%BB%80%E0%BA%AA%E0%BA%94|%E1%83%A1%E1%83%9E%E1%83%94%E1%83%AA%E1%83%98%E1%83%90%E1%83%9A%E1%83%A3%E1%83%A0%E1%83%98|%E1%88%8D%E1%8B%A9|%E1%9E%96%E1%9E%B7%E1%9E%9F%E1%9F%81%E1%9E%9F|%E7%89%B9%E5%88%A5|%E7%89%B9%E6%AE%8A|%ED%8A%B9%EC%88%98|Erenoam%C3%A1%C5%A1|Erenoam\u00e1\u0161|Eri|Er_lheh|Especial|Espesial|Espesiat|Espesi%C3%A1l|Espesi\u00e1l|Espesyal|Extra|Husus|Ih%C3%BC_k%C3%A1r%C3%ADr%C3%AD|Ih\u00fc_k\u00e1r\u00edr\u00ed|Immikkut|Ispetziale|Istimewa|Istimiwa|Jagleel|Kerfiss%C3%AD%C3%B0a|Kerfiss\u00ed\u00f0a|khaas|Khas|Kusuih|Maalum|Maasus|Mahsus|Manokana|Maxsus|Mba%27ech%C4%A9ch%C4%A9|Mba'ech\u0129ch\u0129|Natatangi|N%C5%8Dncuahqu%C4%ABzqui|N\u014dncuahqu\u012bzqui|Papa_nui|Patikos|P%C3%A0t%C3%A0k%C3%AC|Pinaurog|Posebno|P\u00e0t\u00e0k\u00ec|Sapaq|S%C3%B2nraichte|Schbezial|Serstakt|Sevi%C5%A1kuo|Sevi\u0161kuo|Sipeci%C3%A5s|Sipeci\u00e5s|Sipesol|Soronko|Sp%C3%A8ci%C3%A2l|Sp%C3%A9cial|Spe%C3%A7iale|Spe%C4%8Bjali|Specala|Spec%C4%93los|Speciaal|Special|Speciala%C4%B5o|Speciala\u0135o|Speciale|Specialine|Specialis|Specialne|Specialnje|Specialus|Speciaol|Speci%C3%A0le|Speci%C3%A1lis|Speci%C3%A1ln%C3%AD|Speci%C3%A2l|Speciel|Specioal|Speci\u00e0le|Speci\u00e1lis|Speci\u00e1ln\u00ed|Speci\u00e2l|Specjalna|Specjaln%C3%B4|Specjaln\u00f4|Spec\u0113los|Speisialta|Spesiaal|Spesial|Spesyal|Spe\u00e7iale|Spe\u010bjali|Spezial|Spiciali|Sp\u00e8ci\u00e2l|Sp\u00e9cial|S\u00f2nraichte|Syndrig|Szpecyjalna|Tallituslehek%C3%BClg|Tallituslehek\u00fclg|Taybet|Tek-pia%CC%8Dt|Tek-pia\u030dt|Toiminnot|\u00d6zel|\u00dd\u00f6rite|\u0110\u1eb7c_bi\u1ec7t|\u0160peci\u00e1lne|\u0395\u03b9\u03b4\u03b9\u03ba\u03cc|\u0395\u03b9\u03b4\u03b9\u03ba\u03cc\u03bd|\u0410\u0434\u043c\u044b\u0441\u043b\u043e\u0432\u0430\u0435|\u0410\u043d\u0430\u043b\u043b\u0430\u0430\u0445|\u0410\u0440\u043d\u0430\u0439\u044b|\u0410\u0442\u0430\u0439\u044b\u043d|\u0411\u0430\u0448\u043a\u0430|\u0411\u0430\u0448\u043a\u0430_\u0442\u0435\u0432\u0435\u043d\u044c|\u0411\u0435\u043b\u0445\u0430\u043d|\u0412\u0438\u0436\u0430|\u041a\u044a\u0443\u043b\u043b\u0443\u0433\u044a\u0438\u0440\u0430\u043b_\u043b\u0430\u0436\u0438\u043d|\u041a\u044a\u0443\u043b\u043b\u0443\u043a\u044a|\u041a\u04e9\u0434\u043b\u0445\u043d\u04d9|\u041b\u04f1\u043c\u044b\u043d_\u044b\u0448\u0442\u044b\u043c\u0435|\u041c\u0430\u0445\u0441\u0443\u0441|\u041d\u0430\u0440\u043e\u0447\u044c\u043d\u0430|\u041e\u0442\u0441\u0430\u0441\u044f\u043d|\u041f\u0430\u043d\u0435\u043b\u044c|\u041f\u043e\u0441\u0435\u0431\u043d\u043e|\u0421\u00e6\u0440\u043c\u0430\u0433\u043e\u043d\u0434|\u0421\u043b\u0443\u0436\u0435\u0431\u043d\u0430\u044f|\u0421\u043f\u0435\u0446\u0438\u0430\u043b\u043d\u0438|\u0421\u043f\u0435\u0446\u0438\u0458\u0430\u043b\u043d\u0430|\u0421\u043f\u0435\u0446\u0456\u0430\u043b\u044c\u043d\u0430|\u0421\u043f\u0435\u0446\u04f9\u043b\u04f9\u0448\u0442\u04d3\u0448|\u0421\u043f\u044d\u0446\u044b\u044f\u043b\u044c\u043d\u044b\u044f|\u0422\u0443\u0441\u0433\u0430\u0439|\u0422\u0443\u0441\u043a\u0430\u0439|\u0422\u0443\u0441\u0445\u0430\u0439|\u0426\u0430\u0441\u0442\u04d9\u0438|\u0428\u043f\u0435\u0446\u0456\u0430\u043b\u043d\u0430|\u042f\u0442\u0430\u0440\u043b\u0103|\u054d\u057a\u0561\u057d\u0561\u0580\u056f\u0578\u0572|\u05d1\u05d0\u05b7\u05d6\u05d5\u05e0\u05d3\u05e2\u05e8|\u05de\u05d9\u05d5\u05d7\u05d3|\u0626\u0627\u0644\u0627\u06be\u0649\u062f\u06d5|\u062a\u0627\u06cc\u0628\u06d5\u062a|\u062e\u0627\u0635|\u0634\u0627|\u0648\u06cc\u0698\u0647|\u0681\u0627\u0646\u06ab\u0693\u06cc|\u0715\u071d\u0720\u0722\u071d\u0710|\u079a\u07a7\u0787\u07b0\u0790\u07a6|\u0935\u093f\u0936\u0947\u0937|\u0935\u093f\u0936\u0947\u0937\u092e\u094d|\u0935\u093f\u0938\u0947\u0938|\u09ac\u09bf\u09b6\u09c7\u09b7|\u0a16\u0a3c\u0a3e\u0a38|\u0ab5\u0abf\u0ab6\u0ac7\u0ab7|\u0b2c\u0b3f\u0b36\u0b47\u0b37|\u0b9a\u0bbf\u0bb1\u0baa\u0bcd\u0baa\u0bc1|\u0c2a\u0c4d\u0c30\u0c24\u0c4d\u0c2f\u0c47\u0c15|\u0cb5\u0cbf\u0cb6\u0cc7\u0cb7|\u0d2a\u0d4d\u0d30\u0d24\u0d4d\u0d2f\u0d47\u0d15\u0d02|\u0dc0\u0dd2\u0dc1\u0dda\u0dc2|\u0e1e\u0e34\u0e40\u0e28\u0e29|\u0e9e\u0eb4\u0ec0\u0eaa\u0e94|\u10e1\u10de\u10d4\u10ea\u10d8\u10d0\u10da\u10e3\u10e0\u10d8|\u120d\u12e9|\u1796\u17b7\u179f\u17c1\u179f|\u7279\u5225|\u7279\u6b8a|\ud2b9\uc218|Uslig|Uzalutno|Wiki|X%C3%BCsusi|X\u00fcsusi"
	        + "):.*");

	// An ad-hoc pattern to catch bots beyond those captured by the ua_parser library.
	private static final Pattern FORBIDDEN_UA_PATTERN = Pattern
	    .compile(".*([Bb]ot|[Ss]pider|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks).*|Java/1\\..*|curl.*|PHP/.*|-|");

	public static final String[] INPUT_FIELDS = { BrowserEvent.JSON_DT, BrowserEvent.JSON_IP,
	    BrowserEvent.JSON_HTTP_STATUS, BrowserEvent.JSON_URI_HOST, BrowserEvent.JSON_URI_PATH,
	    BrowserEvent.JSON_URI_QUERY, BrowserEvent.JSON_REFERER, BrowserEvent.JSON_XFF,
	    BrowserEvent.JSON_UA, BrowserEvent.JSON_ACCEPT_LANG };

	private static enum HADOOP_COUNTERS {
		MAP_SKIPPED_BAD_HOST, MAP_SKIPPED_BAD_PATH, MAP_SKIPPED_SPECIAL_PAGE, MAP_SKIPPED_BOT, MAP_SKIPPED_BAD_HTTP_STATUS, MAP_OK_REQUEST, MAP_EXCEPTION
	}

	// A regex of the Wikimedia sites we're interested in, e.g., "(pt|es)\\.wikipedia\\.org".
	// Produced from the languagePattern configuration parameter.
	private Pattern uriHostPattern;
	// A parser for determining whether requests come from a bot.
	private Parser uaParser;

	@Override
	public void setup(Context context) {
		uriHostPattern = Pattern.compile(String.format("(%s)\\.wikipedia\\.org", context
		    .getConfiguration().get(CONF_LANGUAGE_PATTERN, "")));
		try {
			uaParser = new Parser();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// Extract the last IP address from the x_forwarded_for string. If the result doesn't look like
	// an IP address (because it contains no "." [IPv4] or ":" [IPv6]), return null.
	private static String processXForwardedFor(String xff) {
		int lastCommaIdx = xff.lastIndexOf(", ");
		String ip = lastCommaIdx >= 0 ? xff.substring(lastCommaIdx + 2) : xff;
		// NB: The ":" part is untested.
		// ////////////////////////////// UNTESTED //////////////////////////////////////
		if (ip.contains(".") || ip.contains(":")) {
			return ip;
		} else {
			return null;
		}
	}

	private boolean isBot(String userAgent) {
		return uaParser.parseDevice(userAgent).family.equals("Spider")
		    || FORBIDDEN_UA_PATTERN.matcher(userAgent).matches();
	}

	private static String extractLanguage(String uriHost) {
		return uriHost.substring(0, uriHost.indexOf('.'));
	}

	// We want to send everything the same user did to the same reducer.
	// Users are represented as the triple (ip, x_forwarded_for, user_agent).
	protected static String makeKey(JSONObject json, String lang) throws JSONException {
		String ip = json.getString(BrowserEvent.JSON_IP);
		String ua = json.getString(BrowserEvent.JSON_UA);
		String acceptLang = json.getString(BrowserEvent.JSON_ACCEPT_LANG);
		String xff = processXForwardedFor(json.getString(BrowserEvent.JSON_XFF));
		// If x_forwarded_for contains an IP address, use that address, otherwise use the address from
		// from the ip field.
		String ipForKey = (xff == null) ? ip : xff;
		// Just in case, replace tabs, so we don't mess with the key/value split.
		return String.format("%s%s%s%s%s%s%s", lang, UID_SEPARATOR, ipForKey, UID_SEPARATOR, ua,
		    UID_SEPARATOR, acceptLang).replace('\t', ' ');
	}

	private static JSONObject createJson(Group data) {
		JSONObject json = new JSONObject();
		for (String field : INPUT_FIELDS) {
			json.put(field, data.getString(field, 0));
		}
		// ////////////////////////////// UNTESTED //////////////////////////////////////
		// Add geocoded data (if specified in input schema, cf. TextExtraction.getSchema()).
		if (data.getFieldRepetitionCount("geocoded_data") > 0) {
			Group geo = data.getGroup("geocoded_data", 0);
			String country = null;
			String state = null;
			String city = null;
			String lat = null;
			String lon = null;
			for (int i = 0; i < geo.getFieldRepetitionCount("map"); ++i) {
				Group pair = geo.getGroup("map", i);
				String key = pair.getString("key", 0);
				String value = pair.getString("value", 0);
				if (key.equals("country_code")) country = value;
				else if (key.equals("subdivision")) state = value;
				else if (key.equals("city")) city = value;
				else if (key.equals("latitude")) lat = value;
				else if (key.equals("longitude")) lon = value;
			}
			json.put(BrowserEvent.JSON_COUNTRY, country);
			json.put(BrowserEvent.JSON_STATE, state);
			json.put(BrowserEvent.JSON_CITY, city);
			json.put(BrowserEvent.JSON_LATLON, String.format("%s,%s", lat, lon));
		}
		return json;
	}

	@Override
	public void map(LongWritable key, Group value, Context context) throws IOException,
	    InterruptedException {
		try {
			JSONObject json = createJson(value);
			// The request must be for one of the whitelisted Wikimedia sites.
			if (!uriHostPattern.matcher(json.getString(BrowserEvent.JSON_URI_HOST)).matches()) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_HOST).increment(1);
				return;
			}
			// It must be to an article page or a wiki search.
			boolean isArticle = json.getString(BrowserEvent.JSON_URI_PATH).startsWith("/wiki/")
			// UNTESTED.
			    && json.getString(BrowserEvent.JSON_URI_QUERY).isEmpty();
			boolean isWikiSearch = json.getString(BrowserEvent.JSON_URI_PATH).equals("/w/index.php")
			    && WikiSearch.QUERY_PATTERN.matcher(json.getString(BrowserEvent.JSON_URI_QUERY))
			        .matches();
			if (!isArticle && !isWikiSearch) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_PATH).increment(1);
				return;
			}
			// Certain page types such as "Special:" pages aren't allowed (but we must make sure we're not
			// also banning the main page).
			if (SPECIAL_NAMESPACE_PATTERN.matcher(json.getString(BrowserEvent.JSON_URI_PATH)).matches()) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_SPECIAL_PAGE).increment(1);
				return;
			}
			// It can't be from a bot.
			if (isBot(json.getString(BrowserEvent.JSON_UA))) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BOT).increment(1);
				return;
			}
			// We only accept HTTP statuses 200 (OK), 304 (Not Modified), and 302 (Found). The latter are
			// important only for wiki searches.
			if (!json.getString(BrowserEvent.JSON_HTTP_STATUS).equals("200")
			    && !json.getString(BrowserEvent.JSON_HTTP_STATUS).equals("304")
			    // UNTESTED.
			    && !(json.getString(BrowserEvent.JSON_HTTP_STATUS).equals("302") && isWikiSearch)) {
				context.getCounter(HADOOP_COUNTERS.MAP_SKIPPED_BAD_HTTP_STATUS).increment(1);
				return;
			}
			// Only if all those filters were passed do we output the row.
			String lang = extractLanguage(json.getString(BrowserEvent.JSON_URI_HOST));
			context.write(new Text(makeKey(json, lang)), new Text(json.toString()));
			context.getCounter(HADOOP_COUNTERS.MAP_OK_REQUEST).increment(1);
		} catch (JSONException e) {
			context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
