package org.wikimedia.west1.baskets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeFlattenMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String CONF_BREAK_BY_DAY = "org.wikimedia.west1.traces.breakByDay";

	private boolean breakByDay;

	private static enum HADOOP_COUNTERS {
		MAP_OK, MAP_EXCEPTION
	}

	@Override
	public void setup(Context context) {
		breakByDay = context.getConfiguration().getBoolean(CONF_BREAK_BY_DAY, false);
	}

	private static String setToString(Set<String> set) {
		StringBuffer buf = new StringBuffer();
		String delim = "";
		for (String s : set) {
			buf.append(delim).append(s);
			delim = "|";
		}
		return buf.toString();
	}

	private static Set<String> flattenTree(JSONObject root) {
		Set<String> set = new HashSet<String>();
		// Discard search queries.
		if (!root.has("is_search")) {
			set.add(root.getString("title"));
		}
		if (root.has("children")) {
			JSONArray children = root.getJSONArray("children");
			for (int i = 0; i < children.length(); ++i) {
				set.addAll(flattenTree(children.getJSONObject(i)));
			}
		}
		return set;
	}

	private String makeKey(String treeId) {
		int i1 = treeId.indexOf('_');
		int i2 = treeId.indexOf('_', i1 + 1);
		int i3 = treeId.indexOf('_', i2 + 1);
		// If trees from different days are to go to different baskets, include the day in the key.
		return treeId.substring(0, breakByDay ? i3 : i2);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
	    InterruptedException {
		try {
			JSONObject json = new JSONObject(value.toString());
			String outKey = makeKey(json.getString("id"));
			String outValue = setToString(flattenTree(json));
			context.write(new Text(outKey), new Text(outValue));
			context.getCounter(HADOOP_COUNTERS.MAP_OK).increment(1);
		} catch (JSONException e) {
			context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
