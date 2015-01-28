#!/bin/bash

export IN_FILE=/user/west1/enwiki-20140102-pages-articles-multistream.xml
export OUT_FILE=/user/west1/enwiki_20140102_redirects

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.5.0-cdh5.2.0.jar \
    -D              org.wikimedia.wikihadoop.previousRevision=false \
    -D              mapreduce.input.fileinputformat.split.minsize=200000000 \
    -libjars        /afs/cs.stanford.edu/u/west1/repo/lib/wikihadoop-0.2.jar \
    -inputformat    org.wikimedia.wikihadoop.StreamWikiDumpInputFormat \
    -input          $IN_FILE \
    -output         $OUT_FILE \
    -files          /afs/cs.stanford.edu/u/west1/repo/code/perl/death/redirect_extractor_mapper.pl \
    -mapper         "/usr/bin/perl ./redirect_extractor_mapper.pl" \
    -numReduceTasks 0
