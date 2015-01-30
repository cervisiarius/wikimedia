#!/bin/bash

# Modify these parameters.
# This is where the JAR file with the Mapper and Reducer code resides.
export TARGET_DIR=~/wikimedia/trunk/target
# The part of the server logs you want to process.
export IN_DIR=/wmf/data/raw/webrequest/webrequest_text/hourly/2015/01/18/09/webrequest_text.22.0.*
# The output directory.
export OUT_DIR=/user/west1/tree_extractor_test_BOT-TEST
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodPageview().
export KEEP_BAD_TREES=false
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.getMinimumSpanningForest().
export KEEP_AMBIGUOUS_TREES=true
# Regular expression of the uri_hosts you want to include.
export URI_HOST_PATTERN='pt\.wikipedia\.org'
# The page-redirect file. Make sure this corresponds to URI_HOST_PATTERN.
export REDIRECT_FILE=ptwiki_20141104_redirects.tsv.gz

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars     $TARGET_DIR/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D           mapred.child.java.opts=-Xss10m \
    -D           mapreduce.output.fileoutputformat.compress=false \
    -D           mapreduce.input.fileinputformat.split.minsize=300000000 \
    -D           mapreduce.task.timeout=6000000 \
    -D           mapreduce.map.output.key.class=org.apache.hadoop.io.Text \
    -D           mapreduce.map.output.value.class=org.apache.hadoop.io.Text \
    -D           mapreduce.job.output.key.class=org.apache.hadoop.io.Text \
    -D           mapreduce.job.output.value.class=org.apache.hadoop.io.Text \
    -D           org.wikimedia.west1.traces.uriHostPattern=$URI_HOST_PATTERN \
    -D           org.wikimedia.west1.traces.redirectFile=$REDIRECT_FILE \
    -D           org.wikimedia.west1.traces.keepAmbiguousTrees=$KEEP_AMBIGUOUS_TREES \
    -D           org.wikimedia.west1.traces.keepBadTrees=$KEEP_BAD_TREES \
    -D           org.wikimedia.west1.traces.hashSalt=`date +%s | sha256sum | base64 | head -c 64` \
    -inputformat SequenceFileAsTextInputFormat \
    -input       $IN_DIR \
    -output      $OUT_DIR \
    -mapper      org.wikimedia.west1.traces.GroupAndFilterMapper \
    -reducer     org.wikimedia.west1.traces.TreeExtractorReducer \
    -numReduceTasks 100
