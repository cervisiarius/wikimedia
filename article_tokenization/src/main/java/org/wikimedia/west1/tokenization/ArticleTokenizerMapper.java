package org.wikimedia.west1.dump;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ArticleTokenizerMapper implements Mapper<Text, Text, Text, Text> {

  private static enum HADOOP_COUNTERS {
    MAP_OK_REQUEST, MAP_EXCEPTION
  }

  //@Override
  public void configure(JobConf conf) {
  }

  //@Override
  public void close() throws IOException {
  }
  
  //@Override
  public void map(Text jsonString, Text emptyValue, OutputCollector<Text, Text> out,
      Reporter reporter) throws IOException {
    try {
      // Only if all those filters were passed do we output the row.
      out.collect(new Text(), jsonString);
      reporter.incrCounter(HADOOP_COUNTERS.MAP_OK_REQUEST, 1);
    } catch (Exception e) {
      reporter.incrCounter(HADOOP_COUNTERS.MAP_EXCEPTION, 1);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(baos));
      System.err.format("MAP_EXCEPTION: %s\n", baos.toString("UTF-8"));
    }
  }

}
