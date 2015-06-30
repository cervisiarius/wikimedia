package org.wikimedia.west1.simtk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

public class PairCountingReducer extends Reducer<Text, Text, Text, NullWritable> {

  private static final int MAX_NUM_EVENTS = 10000;

  private static enum HADOOP_COUNTERS {
    REDUCE_OK_SESSIONS, REDUCE_TOO_MANY_EVENTS, REDUCE_EXCEPTION
  }

  private static String makePairString(String path1, String path2) {
    path1 = path1.endsWith("/") ? path1 : path1 + "/";
    path2 = path2.endsWith("/") ? path2 : path2 + "/";
    if (path1.equals(path2)) {
      throw new IllegalArgumentException();
    }
    return path1 + "\t" + path2;
  }

  private void processSession(List<BrowserEvent> session,
      Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException,
      InterruptedException {
    Set<String> direct = new HashSet<String>();
    Set<String> all = new HashSet<String>();
    for (int i = 0; i < session.size(); ++i) {
      BrowserEvent e1 = session.get(i);
      for (int j = i + 1; j < session.size(); ++j) {
        BrowserEvent e2 = session.get(j);
        // TODO: need to make sure the link source is also in this session.
        try {
          all.add(makePairString(e1.url.getPath(), e2.url.getPath()));
        } catch (IllegalArgumentException e) {
          // This happens if source equals target.
        }
        if (e2.referer != null && e2.referer.getPath().startsWith("/home/")) {
          try {
            direct.add(makePairString(e2.referer.getPath(), e2.url.getPath()));
          } catch (IllegalArgumentException e) {
            // This happens if source equals target.
          }
        }
      }
    }
    for (String pair : direct) {
      context.write(new Text("CLICK\t" + pair), NullWritable.get());
    }
    for (String pair : all) {
      context.write(new Text("PATH\t" + pair), NullWritable.get());
    }
    context.getCounter(HADOOP_COUNTERS.REDUCE_OK_SESSIONS).increment(1);
  }

  // Takes a list of events, orders them by time, and chunks them at 1-hour idle times.
  private List<List<BrowserEvent>> sequenceToSessions(List<BrowserEvent> events)
      throws JSONException {
    // This sort is stable, so requests having the same timestamp will stay in the original order.
    Collections.sort(events, new Comparator<BrowserEvent>() {
      public int compare(BrowserEvent e1, BrowserEvent e2) {
        long t1 = e1.time;
        long t2 = e2.time;
        // NB: Don't use subtraction-based comparison, since overflow may cause errors!
        if (t1 > t2)
          return 1;
        else if (t1 < t2)
          return -1;
        else
          return 0;
      }
    });
    List<List<BrowserEvent>> sessions = new ArrayList<List<BrowserEvent>>();
    long lastTime = -1;
    List<BrowserEvent> session = new ArrayList<BrowserEvent>();
    for (BrowserEvent event : events) {
      // If we see a break of more than one hour, flush the last session.
      if (lastTime >= 0 && event.time - lastTime > 3600 * 1000) {
        sessions.add(new ArrayList<BrowserEvent>(session));
        session = new ArrayList<BrowserEvent>();
      }
      session.add(event);
      lastTime = event.time;
    }
    return sessions;
  }

  @Override
  public void reduce(Text key, Iterable<Text> eventsIt,
      Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException {
    try {
      final long MIN_TIME = BrowserEvent.DATE_FORMAT.parse("2014-01-15T00:00:00").getTime();
      List<BrowserEvent> events = new ArrayList<BrowserEvent>();
      int n = 0;
      // Collect all events for this user.
      for (Text eventText : eventsIt) {
        ///////////////////////////////////////////
        if (eventText.toString().contains("/home/zephyr") && eventText.toString().matches(".*/home/(dmd|emma|forcebalance|msmbuilder).*")) {
          System.err.println("!!!!!!!!!!FOUND(1)  " + eventText);
        }
        // If there are too many event events, output nothing.
        if (++n > MAX_NUM_EVENTS) {
          context.getCounter(HADOOP_COUNTERS.REDUCE_TOO_MANY_EVENTS).increment(1);
          return;
        } else {
          JSONObject json = new JSONObject(eventText.toString());
          BrowserEvent event = new BrowserEvent(json);
          // Only consider events from the time after the introduction of the recommendation
          // feature. Also, only consider views of project pages (they start with "/home/").
          if (event.time > MIN_TIME && event.url.getPath().startsWith("/home/")) {
            events.add(event);
            ///////////////////////////////////////////
            if (eventText.toString().contains("/home/zephyr") && eventText.toString().matches(".*/home/(dmd|emma|forcebalance|msmbuilder).*")) {
              System.err.println("!!!!!!!!!!FOUND(2)  " + eventText);
            }
          }
        }
      }
      for (List<BrowserEvent> session : sequenceToSessions(events)) {
        processSession(session, context);
      }
    } catch (Exception e) {
      context.getCounter(HADOOP_COUNTERS.REDUCE_EXCEPTION).increment(1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
