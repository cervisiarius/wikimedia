#!/bin/bash

# Modify these parameters.
MONTH=9

# The part of the server logs you want to process.
IN_DIR=/wmf/data/wmf/webrequest/webrequest_source=text/year=2015/month=$MONTH/*/*/*
#IN_DIR=/wmf/data/wmf/webrequest/webrequest_source=text/year=2015/month=$MONTH/day=1/hour=3/*
# The output directory.
OUT_DIR=/user/west1/hoax_webrequest_logs_WITH-BOTS-AND-404/month=$MONTH
# This is where the JAR file with the Mapper and Reducer code resides.
TARGET_DIR=$HOME/wikimedia/trunk/hoaxes/target
# Logs are written here.
LOG_DIR=$HOME/wikimedia/trunk/data/log

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
hadoop jar $TARGET_DIR/HoaxLogExtractor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D            mapreduce.job.queuename=priority \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.task.timeout=6000000 \
    -D            org.wikimedia.west1.traces.input=$IN_DIR \
    -D            org.wikimedia.west1.traces.output=$OUT_DIR \
    -libjars      $LIBJARS \
2>&1 | tee $LOG_DIR/hoax_log_extraction_month=$MONTH\_`date +%Y%m%dT%H%M%S`.log
