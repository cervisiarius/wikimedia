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
	public String url;
	public String referer;

	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private static final String UTF8 = "UTF-8";

	private static final String decode(String s) {
		try {
			return URLDecoder.decode(s, UTF8);
		} catch (Exception e) {
			return s;
		}
	}

	// URL-decode and resolve redirects.
	private static String normalizePath(String uriPath, Map<String, String> redirects) {
		// URL-decode the path. It's important to do this first, since it might change character
		// indices.
		uriPath = decode(uriPath);
		// Valid paths contain '/wiki/' before the article name.
		int nameStartIdx = uriPath.indexOf("/wiki/") + 6;
		if (nameStartIdx < 0) {
			return uriPath;
		}
		String prefix = uriPath.substring(0, nameStartIdx);
		String name = uriPath.substring(nameStartIdx);
		if (redirects != null) {
			String redirectTarget = redirects.get(name);
			if (redirectTarget != null) {
				uriPath = prefix + redirectTarget;
			}
		}
		return uriPath;
	}

	public Pageview(JSONObject json) throws JSONException, ParseException {
		this(json, null);
	}

	public Pageview(JSONObject json, Map<String, String> redirects) throws JSONException,
	    ParseException {
		this.json = json;
		this.time = DATE_FORMAT.parse(json.getString("dt")).getTime();
		this.seq = json.getLong("sequence");
		// Normalize the URI path. It is stored as modified in the JSON object.
		json.put("uri_path", normalizePath(json.getString("uri_path"), redirects));
		this.url = String.format("%s%s%s", json.getString("uri_host"), json.getString("uri_path"),
		    json.getString("uri_query"));
		// Strip the protocol from the referer and URL decode the path, so it's comparable to the URL.
		try {
			URL ref = new URL(json.getString("referer"));
			String q = ref.getQuery();
			q = q == null ? "" : "?" + q;
			// NB: anchor info ("#...") is omitted.
			this.referer = String.format("%s%s%s", ref.getAuthority().replace("//", ""),
			    normalizePath(ref.getPath(), redirects), q);
		} catch (MalformedURLException e) {
			String[] tokens = json.getString("referer").split("://");
			if (tokens.length > 1) {
				this.referer = normalizePath(tokens[1], redirects);
			} else {
				this.referer = tokens[0];
			}
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
