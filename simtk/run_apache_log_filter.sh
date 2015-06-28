#!/bin/bash

# Modify these parameters.
IN_FILE=/user/west1/simtk/weblogs
OUT_DIR=/user/west1/simtk/weblogs_filtered

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -files        $HOME/wikimedia/trunk/simtk/src/main/perl/filter_apache_logs.pl \
    -mapper       "/usr/bin/perl ./filter_apache_logs.pl" \
    -reducer      "/bin/cat" \
    -numReduceTasks 10
