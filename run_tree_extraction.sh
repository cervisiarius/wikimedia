#!/bin/bash

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars     ~/wikimedia/trunk/target/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D           mapreduce.output.fileoutputformat.compress=false \
    -D           mapreduce.input.fileinputformat.split.minsize=300000000 \
    -D           mapreduce.task.timeout=6000000 \
    -inputformat SequenceFileAsTextInputFormat \
    -jobconf     mapred.mapoutput.key.class=org.apache.hadoop.io.Text \
    -jobconf     mapred.mapoutput.value.class=org.apache.hadoop.io.Text \
    -jobconf     mapred.output.key.class=org.apache.hadoop.io.Text \
    -jobconf     mapred.output.value.class=org.apache.hadoop.io.Text \
    -input       /wmf/data/raw/webrequest/webrequest_text/hourly/2014/12/04/01/webrequest_text.22.8.* \
    -output      "/user/west1/tree_extractor_test" \
    -mapper      org.wikimedia.west1.traces.GroupByUserAndDayMapper \
    -reducer     org.wikimedia.west1.traces.TreeExtractorReducer
