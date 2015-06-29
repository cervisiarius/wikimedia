package org.wikimedia.west1.simtk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class TreeExtractionMapper extends Mapper<Text, NullWritable, Text, Text> {

	private static enum HADOOP_COUNTERS {
		MAP_OK_REQUEST, MAP_EXCEPTION
	}

	@Override
	public void map(Text key, NullWritable value, Context context) throws IOException,
	    InterruptedException {
		try {
			JSONObject json = new JSONObject();
			// ip, date, http_status, path, referer, user_agent.
			String[] tokens = key.toString().split("\t", 6);
			json.put(BrowserEvent.JSON_IP, tokens[0]);
			json.put(BrowserEvent.JSON_DT, tokens[1]);
			json.put(BrowserEvent.JSON_HTTP_STATUS, tokens[2]);
			json.put(BrowserEvent.JSON_PATH, tokens[3]);
			json.put(BrowserEvent.JSON_REFERER, tokens[4]);
			json.put(BrowserEvent.JSON_UA, tokens[5]);

			context.write(
			    new Text(String.format("%s|%s", json.getString(BrowserEvent.JSON_IP),
			        json.getString(BrowserEvent.JSON_UA))), new Text(json.toString()));
			context.getCounter(HADOOP_COUNTERS.MAP_OK_REQUEST).increment(1);
		} catch (JSONException e) {
			context.getCounter(HADOOP_COUNTERS.MAP_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
