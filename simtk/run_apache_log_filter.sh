#!/bin/bash

# Modify these parameters.
IN_FILE=/user/west1/simtk/weblogs
OUT_DIR=/user/west1/simtk/weblogs_filtered
LIB_DIR=$HOME/local-apps/analytics-ua-parser/perl/lib/

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/simtk/src/main/perl/filter_apache_logs.pl \
    -file         $LIB_DIR/Yaml/Tiny.pm \
    -file         $LIB_DIR/HTTP/UA/Parser.pm \
    -file         $LIB_DIR/HTTP/UA/Parser/regexes.yaml \
    -mapper       "/usr/bin/perl ./filter_apache_logs.pl" \
    -reducer      "/bin/cat" \
    -numReduceTasks 10
