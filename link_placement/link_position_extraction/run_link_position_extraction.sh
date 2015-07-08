#!/bin/bash

# Modify these parameters.
# This is where the JAR file with the Mapper code resides.
TARGET_DIR=$HOME/wikimedia/trunk/link_placement/link_position_extraction/target
# This is where additional JARs reside.
LIB_DIR=$HOME/wikimedia/trunk/lib
# The part of the server logs you want to process.
IN_FILE=/user/west1/enwiki-20150304-pages-articles-multistream.xml
# The output directory.
OUT_DIR=/user/west1/wikipedia_link_positions_enwiki-20150304

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars      $TARGET_DIR/LinkPositionExtraction-0.0.1-SNAPSHOT-jar-with-dependencies.jar,$LIB_DIR/wikihadoop-0.2.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.task.timeout=6000000 \
    -D            org.wikimedia.wikihadoop.previousRevision=false \
    -D            mapreduce.input.fileinputformat.split.minsize=200000000 \
    -D            mapreduce.map.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.map.output.value.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.value.class=org.apache.hadoop.io.Text \
    -inputformat  org.wikimedia.wikihadoop.StreamWikiDumpInputFormat \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -mapper       org.wikimedia.west1.traces.LinkPositionExtractionMapper \
    -reducer      org.wikimedia.west1.traces.LinkPositionExtractionReducer \
    -numReduceTasks 10
