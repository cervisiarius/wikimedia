package org.wikimedia.west1.simtk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static enum HADOOP_COUNTERS {
    MAP_OK_REQUEST, MAP_EXCEPTION
  }

  private static final Pattern FORBIDDEN_UA_PATTERN = Pattern
      .compile(".*([Bb]ot|[Cc]rawler|[Ss]pider|[Ss]quider|[Ww]get|HTTrack|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|Abonti|Seznam|RockPeaks).*|Java/.*|curl.*|PHP/.*|-|");

  private static final Pattern ROW_PATTERN = Pattern
      .compile("(\\S*) \\S* \\S* \\[(.*)\\] \"(GET|POST) (.*) HTTP/.*\" (\\d+) \\d+ \"(.*)\" \"(.*)\"");

  private static final Pattern DATE_PATTERN = Pattern
      .compile("(\\d+)/(.+)/(....):(..:..:..) (-?\\d+)");

  private static boolean isSpider(String userAgent) {
    return FORBIDDEN_UA_PATTERN.matcher(userAgent).matches();
  }

  private static boolean isGoodHttpStatus(String status) {
    return status.equals("200") || status.equals("302") || status.equals("304");
  }

  private static boolean isGoodPath(String path) {
    if (path.matches(".*\\.(js|css|png|jpg|jpeg|gif|ico)($|\\?.*)"))
      return false;
    if (path.matches("/(logos|logos-frs|userpics|securimage|templates|themes)/.*"))
      return false;
    if (path.matches("/project/project_following\\.php.*"))
      return false;
    return true;
  }

  private Map<String, String> monthToIntMap;

  @Override
  public void setup(Context context) {
    monthToIntMap = new HashMap<String, String>();
    monthToIntMap.put("Jan", "01");
    monthToIntMap.put("Feb", "02");
    monthToIntMap.put("Mar", "03");
    monthToIntMap.put("Apr", "04");
    monthToIntMap.put("May", "05");
    monthToIntMap.put("Jun", "06");
    monthToIntMap.put("Jul", "07");
    monthToIntMap.put("Aug", "08");
    monthToIntMap.put("Sep", "09");
    monthToIntMap.put("Oct", "10");
    monthToIntMap.put("Nov", "11");
    monthToIntMap.put("Dec", "12");
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    try {
      Matcher m1 = ROW_PATTERN.matcher(value.toString());
      // Row must be valid.
      if (m1.matches()) {
        String ip = m1.group(1);
        String path = m1.group(4);
        String httpStatus = m1.group(5);
        String referer = m1.group(6);
        String userAgent = m1.group(7);
        Matcher m2 = DATE_PATTERN.matcher(m1.group(2));
        if (m2.matches() && !isSpider(userAgent) && isGoodHttpStatus(httpStatus)
            && isGoodPath(path) && !referer.endsWith(path)) {
          String date = String.format("%s-%s-%sT%s", m2.group(3), monthToIntMap.get(m2.group(2)),
              m2.group(1), m2.group(4));
          JSONObject json = new JSONObject();
          json.put(BrowserEvent.JSON_IP, ip);
          json.put(BrowserEvent.JSON_DT, date);
          json.put(BrowserEvent.JSON_HTTP_STATUS, httpStatus);
          json.put(BrowserEvent.JSON_PATH, path);
          json.put(BrowserEvent.JSON_REFERER, referer);
          json.put(BrowserEvent.JSON_UA, userAgent);
          context.write(new Text(String.format("%s|%s", ip, userAgent)), new Text(json.toString()));
          context.getCounter(HADOOP_COUNTERS.MAP_OK_REQUEST).increment(1);
        }
      }
    } catch (JSONException e) {
      context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
