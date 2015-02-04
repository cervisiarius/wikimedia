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
  
  private static final String JSON_URI_PATH = "uri_path";
  private static final String JSON_DT = "dt";
  private static final String JSON_REFERER = "referer";
  private static final String JSON_RESOLVED_URI_PATH = "resolved_uri_path";

  public JSONObject json;
  public long time;
  // The article after redirect resolution.
  public String resolvedArticle;
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
      return decode(uriPath.substring(6));
    } else {
      return decode(uriPath);
    }
  }

  public Pageview(JSONObject json, Map<String, String> redirects) throws JSONException,
      ParseException {
    this.json = json;
    this.time = DATE_FORMAT.parse(json.getString(JSON_DT)).getTime();
    // Normalize the URI path. It is stored as modified in the JSON object.
    String article = extractArticleFromPath(json.getString(JSON_URI_PATH));
    json.put(JSON_URI_PATH, article);
    // Resolve redirects in article.
    String articleRedirect = redirects.get(article);
    if (articleRedirect != null) {
      this.resolvedArticle = articleRedirect;
      json.put(JSON_RESOLVED_URI_PATH, articleRedirect);
    } else {
      this.resolvedArticle = article;
    }
    try {
      URL ref = new URL(json.getString(JSON_REFERER));
      if (ref.getAuthority().endsWith(".wikipedia.org") && ref.getPath().startsWith("/wiki/")) {
        this.refererArticle = extractArticleFromPath(ref.getPath());
        // Resolve redirects in referer.
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
