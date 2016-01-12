package org.wikimedia.west1.baskets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.TreeSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BasketMergeReducer extends Reducer<Text, Text, Text, Text> {

	private static enum HADOOP_COUNTERS {
		REDUCE_OK, REDUCE_EXCEPTION
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

	@Override
	public void reduce(Text key, Iterable<Text> setIt, Reducer<Text, Text, Text, Text>.Context context)
	    throws IOException {
		try {
			TreeSet<String> set = new TreeSet<String>();
			for (Text setString : setIt) {
				for (String page : setString.toString().split("\\|")) {
					set.add(page);
				}
			}
			context.write(key, new Text(setToString(set)));
			context.getCounter(HADOOP_COUNTERS.REDUCE_OK).increment(1);
		} catch (Exception e) {
			context.getCounter(HADOOP_COUNTERS.REDUCE_EXCEPTION).increment(1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			System.err.format("REDUCE_EXCEPTION: %s\n", baos.toString("UTF-8"));
		}
	}

}
