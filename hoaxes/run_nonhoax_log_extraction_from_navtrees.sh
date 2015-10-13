#!/bin/bash

# IMPORTANT: You need to make sure that $HOME/wikimedia/trunk/hoaxes/data/titles.txt
# is linked to the correct list of article titles (hoaxes or nonhoaxes)!
OUT_DIR=/user/west1/nonhoax_webrequest_logs_from_navtrees

# The part of the server logs you want to process.
IN_DIR=/user/ashwinpp/navigation_trees_WITH-SEARCH/month=*/en/*
# Logs are written here.
LOG_DIR=$HOME/wikimedia/trunk/data/log

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            io.compression.codecs=org.apache.hadoop.io.compress.SnappyCodec \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/hoaxes/src/main/python/detect_hoaxes_in_navtrees_mapper.py \
    -file         $HOME/wikimedia/trunk/hoaxes/data/titles.txt \
    -mapper       "/usr/bin/python detect_hoaxes_in_navtrees_mapper.py" \
    -reducer      "/bin/cat" \
    -numReduceTasks 10
2>&1 | tee $LOG_DIR/nonhoax_log_extraction_from_navtrees_`date +%Y%m%dT%H%M%S`.log
