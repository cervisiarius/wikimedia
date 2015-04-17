#!/bin/bash

export LANG=pt
export DATE=20141104

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.5.0-cdh5.2.0.jar \
    -D              org.wikimedia.wikihadoop.previousRevision=false \
    -D              mapreduce.input.fileinputformat.split.minsize=200000000 \
    -libjars        /afs/cs.stanford.edu/u/west1/repo/lib/wikihadoop-0.2.jar \
    -files          /afs/cs.stanford.edu/u/west1/wikimedia/trunk/src/main/perl/redirect_extractor_mapper.pl \
    -inputformat    org.wikimedia.wikihadoop.StreamWikiDumpInputFormat \
    -input          /dataset/wikipedia_dumps/$LANG\wiki-$DATE-pages-articles-multistream.xml.bz2 \
    -output         /user/west1/wikipedia_redirects/$LANG\wiki_$DATE\_redirects \
    -mapper         "/usr/bin/perl ./redirect_extractor_mapper.pl" \
    -numReduceTasks 0
