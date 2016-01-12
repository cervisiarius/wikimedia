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

	private static enum HADOOP_COUNTERS {
		MAP_OK, MAP_EXCEPTION
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
		set.add(root.getString("title"));
		if (root.has("children")) {
			JSONArray children = root.getJSONArray("children");
			for (int i = 0; i < children.length(); ++i) {
				set.addAll(flattenTree(children.getJSONObject(i)));
			}
		}
		return set;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
	    InterruptedException {
		try {
			JSONObject json = new JSONObject(value.toString());
			String id = json.getString("id");
			String uid = id.substring(0, id.indexOf('_', id.indexOf('_') + 1));
			context.write(new Text(uid), new Text(setToString(flattenTree(json))));
			context.getCounter(HADOOP_COUNTERS.MAP_OK).increment(1);
		} catch (JSONException e) {
			context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
