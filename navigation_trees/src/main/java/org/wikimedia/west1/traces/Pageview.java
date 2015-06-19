package org.wikimedia.west1.traces;

import java.text.ParseException;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class Pageview extends BrowserEvent {

  private static final String JSON_UNRESOLVED_TITLE = "unresolved_title";

  // The article after redirect resolution.
  private String resolvedTitle;

  @Override
  public String getPathAndQuery() {
    return "/wiki/" + this.resolvedTitle;
  }

  public Pageview(JSONObject json, Map<String, String> redirects) throws JSONException,
      ParseException {
    super(json, redirects);
    // Extract the article from the URI path.
    String article = extractArticleFromPath(json.getString(JSON_URI_PATH));
    // Resolve redirects in article.
    String articleRedirect = redirects.get(article);
    if (articleRedirect != null) {
      this.resolvedTitle = articleRedirect;
      json.put(JSON_TITLE, articleRedirect);
      json.put(JSON_UNRESOLVED_TITLE, article);
    } else {
      this.resolvedTitle = article;
      json.put(JSON_TITLE, article);
    }
  }

}
