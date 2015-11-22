#!/bin/bash

#IN_FILE=/user/ashwinpp/navigation_trees_WITH-SEARCH/month=1/en
IN_FILE=/user/west1/navigation_trees_JAN-FEB-MAR_2015/month=1/en/part-00999
OUT_DIR=/user/west1/anchor_placement/new_links_in_old_trees

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/anchor_placement/src/main/python/find_new_links_in_old_trees.py \
    -file         $HOME/wikimedia/trunk/data/link_placement/links_added_in_02-15_FILTERED.tsv.gz \
    -mapper       "/usr/bin/python ./find_new_links_in_old_trees.py" \
    -reducer      "/bin/cat" \
    -numReduceTasks 10
