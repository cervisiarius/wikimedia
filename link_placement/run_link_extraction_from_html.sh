#!/bin/bash

# IN_FILE=/user/west1/wiki_parsed/wiki_html_*-of-6.tsv
# OUT_DIR=/user/west1/wiki_parsed/link_positions
IN_FILE=/user/ashwinp/parsed_revisions_20150331000000_combined_try1
OUT_DIR=/user/west1/wiki_parsed/link_positions_20150331000000

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D            mapred.child.java.opts="-Xmx3g" \
    -input        $IN_FILE \
    -output       $OUT_DIR \
    -file         $HOME/wikimedia/trunk/link_placement/src/main/python/extract_links_from_html_mapper.py \
    -file         $HOME/wikimedia/trunk/link_placement/src/main/python/extract_links_from_html_reducer.py \
    -mapper       "/usr/bin/python ./extract_links_from_html_mapper.py" \
    -reducer      "/usr/bin/python ./extract_links_from_html_reducer.py" \
    -numReduceTasks 10
