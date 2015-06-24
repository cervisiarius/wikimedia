package org.wikimedia.west1.traces;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;

public class TestReadParquet extends Configured implements Tool {

	private static final String[] FIELDS = { "dt", "ip", "http_status", "uri_host", "uri_path",
	    "uri_query", "content_type", "referer", "x_forwarded_for", "user_agent", "accept_language" };

	private String getSchema() {
		StringBuffer schema = new StringBuffer();
		schema.append("message webrequest_schema {");
		for (String field : FIELDS) {
			schema.append(String.format("optional binary %s;", field));
		}
		schema.append("};");
		return schema.toString();
	}

	/*
	 * Read a Parquet record
	 */
	public static class MyMap extends Mapper<LongWritable, Group, Text, Text> {

		private static JSONObject createJson(Group data) {
			JSONObject json = new JSONObject();
			for (String field : FIELDS) {
				json.put(field, data.getString(field, 0));
			}
			return json;
		}

		@Override
		public void map(LongWritable key, Group value,
		    Mapper<LongWritable, Group, Text, Text>.Context context) throws IOException,
		    InterruptedException {
			Text outKey = new Text();
			// Get the schema and field values of the record
			String outputRecord = createJson(value).toString();
			// Process the value, create an output record
			// ...
			context.write(outKey, new Text(outputRecord));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("parquet.read.schema", getSchema());

		Job job = new Job(conf);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMap.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		try {
			System.out.println("YIP!");
			int res = ToolRunner.run(new Configuration(), new TestReadParquet(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
}
