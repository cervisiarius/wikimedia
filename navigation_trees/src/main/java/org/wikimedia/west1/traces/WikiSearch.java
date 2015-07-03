package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

public class WikiSearch extends BrowserEvent {

  public static Pattern QUERY_PATTERN = Pattern.compile("\\?search=(.*)&title=[^&]*&?(.*)");

  @Override
  public String getPathAndQuery() {
    return "/w/index.php" + json.getString(JSON_URI_QUERY);
  }

  public WikiSearch(JSONObject json, Map<String, String> redirects) throws JSONException,
      ParseException {
    super(json, redirects);
    // Parse the query string.
    Matcher m = QUERY_PATTERN.matcher(json.getString(JSON_URI_QUERY));
    if (m.matches()) {
      json.put(JSON_IS_SEARCH, true);
      json.put(JSON_TITLE, decode(m.group(1)));
      json.put(JSON_SEARCH_PARAMS, m.group(2));
    } else {
      // UNTESTED.
      throw new IllegalArgumentException("Bad search query string: " + json.toString());
    }
  }

  // For testing only.
  public static void main(String[] args) {
    Matcher m = QUERY_PATTERN.matcher("?search=Hund&title=Special%3ASearch&go=Artikel");
    m.find();
    System.out.println(decode(m.group(1)));
    System.out.println(m.group(2));
  }

}
