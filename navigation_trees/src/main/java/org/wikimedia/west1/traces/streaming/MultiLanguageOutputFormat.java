package org.wikimedia.west1.traces.streaming;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

// Modified from http://stackoverflow.com/questions/18541503/multiple-output-files-for-hadoop-streaming-with-python-mapper
// Use the language code as the key; it will serve as the output directory name and will then be
// discarded
public class MultiLanguageOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	// Use they key as part of the path for the final output file.
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
		return new Path(key.toString(), leaf).toString();
	}

	// We discard the key.
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}
}
