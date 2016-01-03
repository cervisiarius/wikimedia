package org.wikimedia.west1.traces;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public abstract class BrowserEvent {

  // JSON field names.
  public static final String JSON_IP = "ip";
  public static final String JSON_TREE_ID = "id";
  public static final String JSON_CHILDREN = "children";
  public static final String JSON_PARENT_AMBIGUOUS = "parent_ambiguous";
  public static final String JSON_BAD_TREE = "bad_tree";
  public static final String JSON_DT = "dt";
  public static final String JSON_UA = "user_agent";
  public static final String JSON_TITLE = "title";
  public static final String JSON_HTTP_STATUS = "http_status";
  public static final String JSON_REFERER = "referer";
  public static final String JSON_UNRESOLVED_TITLE = "unresolved_title";
  public static final String JSON_IS_SEARCH = "is_search";
  public static final String JSON_SEARCH_PARAMS = "search_params";
  public static final String JSON_URI_QUERY = "uri_query";
  public static final String JSON_URI_PATH = "uri_path";
  public static final String JSON_ANCHOR = "anchor";
  public static final String JSON_URI_HOST = "uri_host";
  public static final String JSON_ACCEPT_LANG = "accept_language";
  public static final String JSON_XFF = "x_forwarded_for";
  public static final String JSON_GEOCODED_DATA = "geocoded_data";

  private static final String UTF8 = "UTF-8";

  protected static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss");

  public JSONObject json;
  public long time;

  private Map<String, String> redirects;

  public BrowserEvent(JSONObject json, Map<String, String> redirects) throws ParseException {
    this.redirects = redirects;
    this.json = json;
    this.time = DATE_FORMAT.parse(json.getString(JSON_DT)).getTime();
  }

  // Creates either a Pageview or a WikiSearch object, depending on the URL.
  public static BrowserEvent newInstance(JSONObject json, Map<String, String> redirects) {
    try {
      if (json.getString(JSON_URI_PATH).startsWith("/wiki/")) {
        return new Pageview(json, redirects);
      } else if (json.getString(JSON_URI_PATH).startsWith("/w/index.php")
          && json.getString(JSON_URI_QUERY).startsWith("?search=")) {
        return new WikiSearch(json, redirects);
      } else {
        throw new IllegalArgumentException();
      }
    } catch (ParseException e) {
      throw new IllegalArgumentException();
    } catch (JSONException e) {
      throw new IllegalArgumentException();
    }
  }

  protected static final String decode(String s) {
    try {
      return URLDecoder.decode(s, UTF8);
    } catch (Exception e) {
      return s;
    }
  }

  // If there's an anchor reference in the URL, it will still be present in the result of this
  // method.
  protected static String extractArticleFromPath(String uriPath) {
    // Valid paths contain '/wiki/' before the article name.
    if (uriPath.startsWith("/wiki/") && uriPath.length() > 6) {
      return decode(uriPath.substring(6));
    } else {
      return decode(uriPath);
    }
  }

  // This returns a representation of the event that can be matched to other events' referer, i.e.,
  // "/wiki/XYZ/" for Pageviews, and "/w/index.php?search=XYZ&title=Special%3ASearch..." for
  // WikiSearches.
  public abstract String getPathAndQuery();

  // This normalizes the referer, such that HTTP vs. HTTPS doesn't matter. For Pageviews, it returns
  // "/wiki/XYZ", where XYZ is the redirect-resolved article name, and for WikiSearches, it returns
  // "/w/index.php?search=...".
  public String getRefererPathAndQuery() {
    try {
      URL ref = new URL(json.getString(JSON_REFERER));
      // If the referer is not from Wikipedia, we return null.
      if (!ref.getAuthority().endsWith(".wikipedia.org")) {
        return null;
      }
      // Else we beat it into shape, so it can be matched to the result of getPathAndQuery().
      else {
        // The referer is an article pageview, so we need to normalize the title.
        if (ref.getPath().startsWith("/wiki/")) {
          String refererArticle = extractArticleFromPath(ref.getPath());
          // Resolve redirects in referer.
          String refererRedirect = redirects.get(refererArticle);
          if (refererRedirect != null) {
            refererArticle = refererRedirect;
          }
          return "/wiki/" + refererArticle;
        }
        // The referer is something else, such as a wiki search.
        else {
          return ref.getPath() + '?' + ref.getQuery();
        }
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
