package org.wikimedia.west1.hoaxes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.hadoop.example.ExampleInputFormat;

public class HoaxLogExtraction extends Configured implements Tool {

	private static final String CONF_INPUT = "org.wikimedia.west1.traces.input";
	private static final String CONF_OUTPUT = "org.wikimedia.west1.traces.output";
	private static final String CONF_NUM_REDUCE = "org.wikimedia.west1.traces.numReduceTasks";

	private String getSchema() {
		StringBuffer schema = new StringBuffer();
		schema.append("message webrequest_schema {");
		for (String field : HoaxLogExtractorMapper.INPUT_FIELDS) {
			schema.append(String.format("optional binary %s;", field));
		}
		schema.append("};");
		return schema.toString();
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		// Make sure we only read the necessary columns.
		conf.set("parquet.read.schema", getSchema());

		Job job = new Job(conf);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

    job.setMapperClass(HoaxLogExtractorMapper.class);
    job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(conf.getInt(CONF_NUM_REDUCE, 1));

		job.setInputFormatClass(ExampleInputFormat.class);
		// This is how we are able to write to several output folders from the same Reducer.
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(conf.get(CONF_INPUT)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(CONF_OUTPUT)));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), new HoaxLogExtraction(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}

}
