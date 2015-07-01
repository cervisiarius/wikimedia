#!/bin/bash

IN_DIR=/user/west1/simtk/weblogs
# The output directory.
OUT_DIR=/user/west1/simtk/pair_counts
# This is where the JAR file with the Mapper and Reducer code resides.
TARGET_DIR=$HOME/wikimedia/trunk/simtk/target
# The number of reducers.
NUM_REDUCE=10

echo "Running hadoop job"
hadoop jar $TARGET_DIR/SimTkPairCounting-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D mapreduce.output.fileoutputformat.compress=false \
    -D org.wikimedia.west1.simtk.input=$IN_DIR \
    -D org.wikimedia.west1.simtk.output=$OUT_DIR \
    -D org.wikimedia.west1.simtk.numReduceTasks=$NUM_REDUCE
