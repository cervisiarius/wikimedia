package org.wikimedia.west1.traces;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class Pageview {
	public JSONObject json;
	public long time;
	public long seq;
	public String article;
	public String refererArticle;

	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private static final String UTF8 = "UTF-8";

	private static final String decode(String s) {
		try {
			return URLDecoder.decode(s, UTF8);
		} catch (Exception e) {
			return s;
		}
	}

	private static String extractArticleFromPath(String uriPath) {
		// Valid paths contain '/wiki/' before the article name.
		if (uriPath.startsWith("/wiki/") && uriPath.length() > 6) {
			return uriPath.substring(6);
		} else {
			return uriPath;
		}
	}

	public Pageview(JSONObject json, Map<String, String> redirects) throws JSONException,
	    ParseException {
		this.json = json;
		this.time = DATE_FORMAT.parse(json.getString("dt")).getTime();
		this.seq = json.getLong("sequence");
		// Normalize the URI path. It is stored as modified in the JSON object.
		json.put("uri_path", decode(json.getString("uri_path")));
		this.article = extractArticleFromPath(json.getString("uri_path"));
		String articleRedirect = redirects.get(this.article);
		if (articleRedirect != null) {
			this.article = articleRedirect;
		}
		try {
			URL ref = new URL(json.getString("referer"));
			if (ref.getAuthority().endsWith(".wikipedia.org") && ref.getPath().startsWith("/wiki/")) {
				this.refererArticle = extractArticleFromPath(decode(ref.getPath()));
				String refererRedirect = redirects.get(this.refererArticle);
				if (refererRedirect != null) {
					this.refererArticle = refererRedirect;
				}
			}
		} catch (MalformedURLException e) {
		}
	}

	public String toString() {
		return json.toString();
	}

	public String toString(int indentFactor) {
		try {
			return json.toString(indentFactor);
		} catch (JSONException e) {
			System.err.println(json);
			return "[JSONException]";
		}
	}
}
