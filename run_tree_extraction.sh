#!/bin/bash

# Modify these parameters.
# This is where the JAR file with the Mapper and Reducer code resides.
export TARGET_DIR=~/wikimedia/trunk/target
# This is where additional JARs reside.
export LIB_DIR=~/wikimedia/trunk/lib
# The part of the server logs you want to process.
#export IN_DIR=/wmf/data/wmf/webrequest/webrequest_source=text/year=2015/*/*/*/*
export IN_DIR=/user/west1/webrequest_source=text/year=2015/*/*/*/*
# The output directory.
export OUT_DIR=/user/west1/navigation_trees_3
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodPageview().
export KEEP_BAD_TREES=false
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodPageview().
export KEEP_SINGLETON_TREES=true
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.getMinimumSpanningForest().
export KEEP_AMBIGUOUS_TREES=true
# Regular expression of the languages you want to include. The following partition of the top 50
# languages into 3 sets was chosen such that the cumulative sizes of their redirect tables are
# approximately equal.
#export LANGUAGE_PATTERN='en'
#export LANGUAGE_PATTERN='es|fr|ru|de|fa|sv|simple|zh|ja'
export LANGUAGE_PATTERN='sh|pt|ar|nl|it|ceb|war|sr|pl|uk|ca|id|ro|tr|ko|no|fi|uz|cs|hu|vi|he|hy|eo|da|bg|et|lt|el|vo|sk|sl|eu|nn|kk|hr|hi|ms|gl|min'
# If a user has more than this many pageviews, we ignore her.
# Having 100K pageviews in a month would mean one every 26 seconds.
# Having 10K pageviews in a month would mean one every 4 minutes.
# Having 3600 pageviews in a day would mean one every 24 seconds.
export MAX_NUM_PAGEVIEWS=10000

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars      $TARGET_DIR/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar,$LIB_DIR/iow-hadoop-streaming-1.0.jar \
    -D            mapred.child.java.opts="-Xss10m -Xmx3g" \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec \
    -D            parquet.read.support.class=net.iponweb.hadoop.streaming.parquet.GroupReadSupport \
    -D            parquet.read.schema="message webrequest_schema {
                                         optional binary dt; 
                                         optional binary ip;
                                         optional binary http_status;
                                         optional binary uri_host;
                                         optional binary uri_path;
                                         optional binary content_type;
                                         optional binary referer;
                                         optional binary x_forwarded_for;
                                         optional binary user_agent;
                                         optional binary accept_language;
                                       }" \
    -D            mapreduce.task.timeout=6000000 \
    -D            mapreduce.map.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.map.output.value.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.value.class=org.apache.hadoop.io.Text \
    -D            org.wikimedia.west1.traces.languagePattern=$LANGUAGE_PATTERN \
    -D            org.wikimedia.west1.traces.keepAmbiguousTrees=$KEEP_AMBIGUOUS_TREES \
    -D            org.wikimedia.west1.traces.keepBadTrees=$KEEP_BAD_TREES \
    -D            org.wikimedia.west1.traces.keepSingletonTrees=$KEEP_SINGLETON_TREES \
    -D            org.wikimedia.west1.traces.hashSalt=`date +%s | sha256sum | base64 | head -c 64` \
    -D            org.wikimedia.west1.traces.maxNumPageviews=$MAX_NUM_PAGEVIEWS \
    -inputformat  net.iponweb.hadoop.streaming.parquet.ParquetAsJsonInputFormat \
    -outputformat org.wikimedia.west1.traces.MultiLanguageOutputFormat \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -mapper       org.wikimedia.west1.traces.GroupAndFilterMapper \
    -reducer      org.wikimedia.west1.traces.TreeExtractorReducer \
    -numReduceTasks 100
