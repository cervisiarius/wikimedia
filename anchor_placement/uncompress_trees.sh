#!/bin/bash

MONTH=1
LANG=en

IN_FILE=/user/ashwinpp/navigation_trees_WITH-SEARCH/month=$MONTH/$LANG
OUT_DIR=/user/west1/navigation_trees_JAN-FEB-MAR_2015/month=$MONTH/$LANG

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -mapper       "/bin/cat" \
    -reducer      "/bin/cat" \
    -numReduceTasks 1000
