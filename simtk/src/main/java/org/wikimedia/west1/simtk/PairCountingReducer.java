package org.wikimedia.west1.simtk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.JSONException;
import org.json.JSONObject;

public class PairCountingReducer extends Reducer<Text, Text, Text, NullWritable> {

	private static final int MAX_NUM_EVENTS = 10000;

	private static Pattern QUERY_PATTERN = Pattern.compile("(\\?feature=rec&rank=\\d+&src=\\d+).*");

	private static enum HADOOP_COUNTERS {
		REDUCE_OK_SESSIONS, REDUCE_TOO_MANY_EVENTS, REDUCE_EXCEPTION
	}

	// private static long REC_FEAT_INTRO_TIME;

	private MultipleOutputs<Text, NullWritable> out;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setup(Reducer<Text, Text, Text, NullWritable>.Context context) {
		out = new MultipleOutputs(context);
		// try {
		// REC_FEAT_INTRO_TIME = BrowserEvent.DATE_FORMAT.parse("2014-01-15T00:00:00").getTime();
		// } catch (ParseException e) {
		// throw new RuntimeException(e);
		// }
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		out.close();
	}

	private static String normalizePath(URL u) {
		String path = u.getPath();
		path = path.endsWith("/") ? path : path + "/";
		if (u.getQuery() != null) {
			Matcher m = QUERY_PATTERN.matcher(u.getQuery());
			if (m.matches()) {
				path = path + m.group(1);
			}
		}
		return path;
	}

	private static String makePairString(URL u1, URL u2) {
		String path1 = normalizePath(u1);
		String path2 = normalizePath(u2);
		if (path1.equals(path2)) {
			throw new IllegalArgumentException();
		}
		return path1 + "\t" + path2;
	}

	private void processSession(List<BrowserEvent> session, String sessionId,
	    Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException,
	    InterruptedException {
		// long sessionTime = session.get(0).time;
		String sessionTime = session.get(0).json.getString(BrowserEvent.JSON_DT);
		Set<String> singletons = new HashSet<String>();
		Set<String> direct = new HashSet<String>();
		Set<String> all = new HashSet<String>();
		for (int i = 0; i < session.size(); ++i) {
			BrowserEvent e1 = session.get(i);
			Set<String> seenPages = new HashSet<String>();
			singletons.add(normalizePath(e1.url));
			for (int j = 0; j < session.size(); ++j) {
				BrowserEvent e2 = session.get(j);
				if (j > i) {
					try {
						all.add(makePairString(e1.url, e2.url));
					} catch (IllegalArgumentException e) {
						// This happens if source equals target.
					}
					// Register a direct click if the referer is also in this session.
					if (e2.referer != null && e2.referer.getPath().startsWith("/home/")
					    && seenPages.contains(e2.referer.getPath())) {
						try {
							direct.add(makePairString(e2.referer, e2.url));
						} catch (IllegalArgumentException e) {
							// This happens if source equals target.
						}
					}
				}
				// Remember the page as seen.
				seenPages.add(e2.url.getPath());
			}
		}
		for (String singleton : singletons) {
			out.write(new Text(singleton + "\t" + sessionTime), NullWritable.get(), "singletons/part");
		}
		for (String pair : direct) {
			// if (sessionTime >= REC_FEAT_INTRO_TIME)
			out.write(new Text(pair + "\t" + sessionTime), NullWritable.get(), "clicks/part");
		}
		for (String pair : all) {
			// if (sessionTime >= REC_FEAT_INTRO_TIME)
			out.write(new Text(pair + "\t" + sessionTime), NullWritable.get(), "pairs/part");
			// else out.write(new Text(pair + "\t" + sessionId), NullWritable.get(), "sessions/part");
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
				sessions.add(session);
				session = new ArrayList<BrowserEvent>();
			}
			session.add(event);
			lastTime = event.time;
		}
		// Take care of last session.
		if (!session.isEmpty()) {
			sessions.add(session);
		}
		return sessions;
	}

	@Override
	public void reduce(Text key, Iterable<Text> eventsIt,
	    Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException {
		try {
			List<BrowserEvent> events = new ArrayList<BrowserEvent>();
			int n = 0;
			// Collect all events for this user.
			for (Text eventText : eventsIt) {
				// If there are too many event events, output nothing.
				if (++n > MAX_NUM_EVENTS) {
					context.getCounter(HADOOP_COUNTERS.REDUCE_TOO_MANY_EVENTS).increment(1);
					return;
				} else {
					JSONObject json = new JSONObject(eventText.toString());
					BrowserEvent event = new BrowserEvent(json);
					// Consider views of project pages (they start with "/home/").
					if (event.url.getPath().startsWith("/home/")) {
						events.add(event);
					}
				}
			}
			int seq = 0;
			for (List<BrowserEvent> session : sequenceToSessions(events)) {
				processSession(session, String.format("%s_%d", key.toString(), seq), context);
				++seq;
			}
		} catch (Exception e) {
			context.getCounter(HADOOP_COUNTERS.REDUCE_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
