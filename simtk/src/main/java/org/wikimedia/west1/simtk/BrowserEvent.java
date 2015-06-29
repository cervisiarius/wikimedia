package org.wikimedia.west1.simtk;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONException;
import org.json.JSONObject;

public class BrowserEvent {

	// JSON field names.
	public static final String JSON_IP = "ip";
	public static final String JSON_TREE_ID = "id";
	public static final String JSON_CHILDREN = "children";
	public static final String JSON_PARENT_AMBIGUOUS = "parent_ambiguous";
	public static final String JSON_DT = "dt";
	public static final String JSON_UA = "user_agent";
	public static final String JSON_PATH = "path";
	public static final String JSON_HTTP_STATUS = "http_status";
	public static final String JSON_REFERER = "referer";
	public static final String JSON_URI_QUERY = "uri_query";
	public static final String JSON_URI_PATH = "uri_path";
	public static final String JSON_ANCHOR = "anchor";
	public static final String JSON_BAD_TREE = "bad_tree";

	protected static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
	    "yyyy-MM-dd'T'HH:mm:ss");

	public JSONObject json;
	public long time;
	public URL url;

	public BrowserEvent(JSONObject json) throws ParseException {
		this.json = json;
		this.time = DATE_FORMAT.parse(json.getString(JSON_DT)).getTime();
		try {
			this.url = new URL("http://simtk.org" + json.getString(JSON_PATH));
			this.json.put(JSON_URI_PATH, url.getPath());
			this.json.put(JSON_URI_QUERY, '?' + url.getQuery());
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException();
		}
	}

	public String getPathAndQuery() {
		return url.getPath() + '?' + url.getQuery();
	}

	// This normalizes the referer, such that HTTP vs. HTTPS doesn't matter.
	public String getRefererPathAndQuery() {
		try {
			URL ref = new URL(json.getString(JSON_REFERER));
			// If the referer is not from SimTk, we return null.
			if (!ref.getAuthority().endsWith("simtk.org")) {
				return null;
			}
			// Else we beat it into shape, so it can be matched to the result of getPathAndQuery().
			else {
				return ref.getPath() + '?' + ref.getQuery();
			}
		} catch (MalformedURLException e) {
			return null;
		} catch (NullPointerException e) {
			// This may happen if ref.getAuthority() or ref.getPath() returns null.
			return null;
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
