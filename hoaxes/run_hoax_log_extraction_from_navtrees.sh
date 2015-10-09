#!/bin/bash

# Modify these parameters.
MONTH=1

# The part of the server logs you want to process.
IN_DIR=/user/ashwinpp/navigation_trees_WITH-SEARCH/month=$MONTH/en/part-00099.snappy
# The output directory.
OUT_DIR=/user/west1/hoax_webrequest_logs_from_navtrees/month=$MONTH
# Logs are written here.
LOG_DIR=$HOME/wikimedia/trunk/data/log

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/hoaxes/src/main/python/detect_hoaxes_in_navtrees_mapper.py \
    -file         $HOME/wikimedia/trunk/hoaxes/data/hoax_titles.txt \
    -mapper       "/usr/bin/python detect_hoaxes_in_navtrees_mapper.py" \
    -reducer      "/bin/cat" \
    -numReduceTasks 10
2>&1 | tee $LOG_DIR/hoax_log_extraction_from_navtrees_month=$MONTH\_`date +%Y%m%dT%H%M%S`.log

#    -reducer      "/usr/bin/sort -t \$'\\t' -k8,8 -k7,7" \
