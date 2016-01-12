#!/bin/bash

# Modify these parameters.
LANG='en'

# The tree data you want to process.
IN_DIR=/user/west1/navigation_trees/year=2015/month=1[12]/$LANG/*
# The output directory.
OUT_DIR=/user/west1/user_baskets/$LANG
# This is where the JAR file with the Mapper and Reducer code resides.
TARGET_DIR=$HOME/wikimedia/trunk/user_basket_extraction/target
# Logs are written here.
LOG_DIR=$HOME/wikimedia/trunk/data/log
# The number of reducers.
NUM_REDUCE=100

QUEUE=default
#QUEUE=priority

echo "Running hadoop job"
hadoop jar $TARGET_DIR/UserBasketExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D            mapreduce.job.queuename=$QUEUE \
    -D            mapred.child.java.opts="-Xss10m -Xmx4g" \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.task.timeout=6000000 \
    -D            dfs.replication=2 \
    -D            org.wikimedia.west1.traces.input=$IN_DIR \
    -D            org.wikimedia.west1.traces.output=$OUT_DIR \
    -D            org.wikimedia.west1.traces.numReduceTasks=$NUM_REDUCE \
2>&1 | tee $LOG_DIR/basket_extraction_lang=$LANG\_`date +%Y%m%dT%H%M%S`.log