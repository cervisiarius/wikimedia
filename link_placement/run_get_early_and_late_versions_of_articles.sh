#!/bin/bash

IN_FILE=/user/west1/revision_dump_link_deltas_20150403/*
OUT_DIR=/user/west1/early_and_late_versions_of_articles

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/link_placement/src/main/perl/get_early_and_late_versions_of_articles.pl \
    -mapper       "/bin/cat" \
    -reducer      "/usr/bin/sort -k1,1 -k5,5 | /usr/bin/perl ./get_early_and_late_versions_of_articles.pl" \
    -numReduceTasks 50
