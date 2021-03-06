#!/bin/bash

# Specify the date range you want to process, e.g., year=2015/month=7/day=5
DATEDIR=$1

if [ -z "$DATEDIR" ] ; then
    echo 'Please enter a valid DATEDIR' 1>&2
    exit
fi

# Modify these parameters.
# Regular expression of the languages you want to include. The following partition of the top 50
# languages into 3 sets was chosen such that the cumulative sizes of their redirect tables are
# approximately equal.
LANGUAGE_PATTERN='en'
#LANGUAGE_PATTERN='es|fr|ru|de|sh|sv|hi|zh|ja'
#LANGUAGE_PATTERN='fa|pt|ar|nl|it|ceb|war|sr|pl|uk|ca|id|ro|tr|ko|no|fi|uz|cs|hu|vi|he|hy|eo|da|bg|et|lt|el|vo|sk|sl|eu|nn|kk|hr|simple|ms|gl|min'

# The part of the server logs you want to process.
IN_DIR=/wmf/data/wmf/webrequest/webrequest_source=text/$DATEDIR
# The output directory.
OUT_DIR=/user/west1/navigation_trees/$DATEDIR
# This is where the JAR file with the Mapper and Reducer code resides.
TARGET_DIR=$HOME/wikimedia/trunk/navigation_trees/target
# Logs are written here.
LOG_DIR=$HOME/wikimedia/trunk/data/log/daily_tree_extraction
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodEvent().
KEEP_BAD_TREES=false
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.isGoodEvent().
KEEP_SINGLETON_TREES=true
# Cf. org.wikimedia.west1.traces.TreeExtractorReducer.getMinimumSpanningForest().
KEEP_AMBIGUOUS_TREES=true
# If a user has more than this many events, we ignore her.
# Having 100K events in a month would mean one every 26 seconds.
# Having 10K events in a month would mean one every 4 minutes.
# Having 3600 events in a day would mean one every 24 seconds.
MAX_NUM_EVENTS=10000
# The number of reducers.
NUM_REDUCE=100

# Set some required environment variables (Thrift is needed for reading Parquet).
if [ -e /opt/cloudera/parcels/CDH ] ; then
    CDH_BASE=/opt/cloudera/parcels/CDH
else
    CDH_BASE=/usr
fi
THRIFTJAR=`ls -l $CDH_BASE/lib/hive/lib/libthrift*jar | awk '{print $9}' | head -1`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$THRIFTJAR
export LIBJARS=$THRIFTJAR

echo "Running hadoop job"
hadoop jar $TARGET_DIR/TreeExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D            mapred.child.java.opts="-Xss10m -Xmx3g" \
    -D            mapreduce.output.fileoutputformat.compress=false \
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
    -libjars      $LIBJARS \
2>&1 | tee $LOG_DIR/tree_extraction_lang=`echo $LANGUAGE_PATTERN | tr '|' '-'`_`echo $DATEDIR | tr '/' '_'`_`date +%Y%m%dT%H%M%S`.log

#    -D            mapreduce.job.queuename=priority \
