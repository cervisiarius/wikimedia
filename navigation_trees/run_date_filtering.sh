#!/bin/bash

# Modify these parameters.
export LANG=en
# The part of the server logs you want to process.
export IN_FILE=/user/west1/navigation_trees/FIRST_RUN/$LANG
# The output directory.
export OUT_DIR=/user/west1/navigation_trees/month=1/$LANG

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.task.timeout=6000000 \
    -files        /home/west1/wikimedia/trunk/navigation_trees/src/main/python/date_filter_mapper.py \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -mapper       "/usr/bin/python ./date_filter_mapper.py" \
    -reducer      "/bin/cat"
