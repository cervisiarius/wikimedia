#!/bin/bash

# Modify these parameters.
# This is where the JAR file with the Mapper and Reducer code resides.
export TARGET_DIR=$HOME/wikimedia/trunk/article_tokenization/target
# The part of the server logs you want to process.
export IN_DIR=/user/west1/wikipedia_dumps/$WIKI-pages-articles-multistream.xml
# The output directory.
export OUT_DIR=/user/west1/wikipedia_plaintexts/$WIKI.tsv

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars      $TARGET_DIR/ArticleTokenization-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    -D            mapreduce.output.fileoutputformat.compress=false \
    -D            mapreduce.task.timeout=6000000 \
    -D            org.wikimedia.wikihadoop.previousRevision=false \
    -D            mapreduce.input.fileinputformat.split.minsize=200000000 \
    -inputformat  org.wikimedia.wikihadoop.StreamWikiDumpInputFormat \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -mapper       org.wikimedia.west1.dump.ArticleTokenizerMapper \
    -reducer      "/bin/cat" \
    -numReduceTasks 100
