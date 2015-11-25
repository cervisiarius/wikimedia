#!/bin/bash

#IN_FILE=/user/ashwinpp/navigation_trees_WITH-SEARCH/month=1/en
IN_FILE=/user/west1/navigation_trees_JAN-FEB-MAR_2015/month=1/en
OUT_DIR=/user/west1/paths_from_trees

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/navigation_trees/src/main/python/trees_to_paths_mapper.py \
    -file         $HOME/wikimedia/trunk/navigation_trees/src/main/python/trees_to_paths_reducer.py \
    -mapper       "/usr/bin/python ./trees_to_paths_mapper.py" \
    -reducer      "/usr/bin/python ./trees_to_paths_reducer.py" \
    -numReduceTasks 10
