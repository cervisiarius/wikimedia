package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONException;
import org.json.JSONObject;

public class Pageview {
	public JSONObject json;
	public long time;
	public long seq;
	public String url;

	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public Pageview(JSONObject json) throws JSONException, ParseException {
		this.json = json;
		this.time = DATE_FORMAT.parse(json.getString("dt")).getTime();
		this.seq = json.getLong("sequence");
		this.url = String.format("http://%s%s%s", json.getString("uri_host"),
		    json.getString("uri_path"), json.getString("uri_query"));
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
