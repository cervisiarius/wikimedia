package org.wikimedia.west1.baskets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserBasketExtraction extends Configured implements Tool {

	private static final String CONF_INPUT = "org.wikimedia.west1.traces.input";
	private static final String CONF_OUTPUT = "org.wikimedia.west1.traces.output";
	private static final String CONF_NUM_REDUCE = "org.wikimedia.west1.traces.numReduceTasks";

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = new Job(conf);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TreeFlattenMapper.class);
		// Since our reduce operation is associative and commutative, we use a Combiner.
		job.setCombinerClass(BasketMergeReducer.class);
		job.setReducerClass(BasketMergeReducer.class);
		job.setNumReduceTasks(conf.getInt(CONF_NUM_REDUCE, 1));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		 
		FileInputFormat.setInputPaths(job, new Path(conf.get(CONF_INPUT)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(CONF_OUTPUT)));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), new UserBasketExtraction(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}

}
