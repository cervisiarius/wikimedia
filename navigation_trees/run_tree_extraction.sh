#!/bin/bash

# Modify these parameters.
# This is where the JAR file with the Mapper and Reducer code resides.
TARGET_DIR=$HOME/wikimedia/trunk/navigation_trees/target
# The part of the server logs you want to process.
#export IN_DIR=/wmf/data/wmf/webrequest/webrequest_source=text/year=2015/*/*/*/*
IN_DIR=/user/west1/webrequest_source=text/year=2015/month=2/day=6/hour=9/000063_0
# The output directory.
OUT_DIR=/user/west1/parquet_test
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodEvent().
KEEP_BAD_TREES=false
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodEvent().
KEEP_SINGLETON_TREES=true
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.getMinimumSpanningForest().
KEEP_AMBIGUOUS_TREES=true
# Regular expression of the languages you want to include. The following partition of the top 50
# languages into 3 sets was chosen such that the cumulative sizes of their redirect tables are
# approximately equal.
LANGUAGE_PATTERN='en'
#export LANGUAGE_PATTERN='es|fr|ru|de|fa|sv|simple|zh|ja'
#export LANGUAGE_PATTERN='sh|pt|ar|nl|it|ceb|war|sr|pl|uk|ca|id|ro|tr|ko|no|fi|uz|cs|hu|vi|he|hy|eo|da|bg|et|lt|el|vo|sk|sl|eu|nn|kk|hr|hi|ms|gl|min'
# If a user has more than this many events, we ignore her.
# Having 100K events in a month would mean one every 26 seconds.
# Having 10K events in a month would mean one every 4 minutes.
# Having 3600 events in a day would mean one every 24 seconds.
MAX_NUM_EVENTS=10000
# The number of reducers.
NUM_REDUCE=100

# Set some required environment variables.
if [ -e /opt/cloudera/parcels/CDH ] ; then
    CDH_BASE=/opt/cloudera/parcels/CDH
else
    CDH_BASE=/usr
fi
THRIFTJAR=`ls -l $CDH_BASE/lib/hive/lib/libthrift*jar | awk '{print $9}' | head -1`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$THRIFTJAR
export LIBJARS=$THRIFTJAR
#export LIBJARS=`echo "$CLASSPATH" | awk 'BEGIN { RS = ":" } { print }' | grep parquet-format | tail -1`
#export LIBJARS=$LIBJARS,$THRIFTJAR

echo $LIBJARS

echo "Running hadoop job"
hadoop jar $TARGET_DIR/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D            mapreduce.job.queuename=priority \
    -D            mapred.child.java.opts="-Xss10m -Xmx3g" \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec \
    -D            mapreduce.task.timeout=6000000 \
    -D            org.wikimedia.west1.traces.languagePattern=$LANGUAGE_PATTERN \
    -D            org.wikimedia.west1.traces.keepAmbiguousTrees=$KEEP_AMBIGUOUS_TREES \
    -D            org.wikimedia.west1.traces.keepBadTrees=$KEEP_BAD_TREES \
    -D            org.wikimedia.west1.traces.keepSingletonTrees=$KEEP_SINGLETON_TREES \
    -D            org.wikimedia.west1.traces.hashSalt=df765889fdiiohfjsfughc2387hsvtrvkjjkhgdsfsdlkfhs74uisafjsdbjb \
    -D            org.wikimedia.west1.traces.maxNumEvents=$MAX_NUM_EVENTS \
    -D            org.wikimedia.west1.traces.input=$IN_DIR \
    -D            org.wikimedia.west1.traces.output=$OUT_DIR \
    -D            org.wikimedia.west1.traces.numReduceTasks=$NUM_REDUCE \
    -libjars      $LIBJARS

# Hash salt used to be generated randomly, such that for different runs, different "users" will get
# different ids (i.e., same language has different ids for different months). But from now on we
# set it deterministically, so we can group users across all 3 months.
#    -D            org.wikimedia.west1.traces.hashSalt=`date +%s | sha256sum | base64 | head -c 64` \
