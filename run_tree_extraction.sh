#!/bin/bash

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars     ~/wikimedia/trunk/target/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D           mapred.child.java.opts=-Xss10m \
    -D           mapreduce.output.fileoutputformat.compress=false \
    -D           mapreduce.input.fileinputformat.split.minsize=300000000 \
    -D           mapreduce.task.timeout=6000000 \
    -D           mapreduce.map.output.key.class=org.apache.hadoop.io.Text \
    -D           mapreduce.map.output.value.class=org.apache.hadoop.io.Text \
    -D           mapreduce.job.output.key.class=org.apache.hadoop.io.Text \
    -D           mapreduce.job.output.value.class=org.apache.hadoop.io.Text \
    -D           org.wikimedia.west1.traces.uriHostPattern='pt\.wikipedia\.org' \
    -D           org.wikimedia.west1.traces.keepAmbiguousTrees=true \
    -D           org.wikimedia.west1.traces.keepBadTrees=true \
    -D           org.wikimedia.west1.traces.hashSalt=jhfsdkf \
    -inputformat SequenceFileAsTextInputFormat \
    -input       /wmf/data/raw/webrequest/webrequest_text/hourly/2015/01/18/09/webrequest_text.22.8.* \
    -output      "/user/west1/tree_extractor_test_BOT-TEST" \
    -mapper      org.wikimedia.west1.traces.GroupAndFilterMapper \
    -reducer     org.wikimedia.west1.traces.TreeExtractorReducer \
    -numReduceTasks 100

#     -input       /wmf/data/raw/webrequest/webrequest_text/hourly/2014/12/04/*/webrequest* \
#     -input       /wmf/data/raw/webrequest/webrequest_text/hourly/2014/12/04/07/webrequest_text.22.0.* \
