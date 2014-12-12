#!/bin/bash

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    #-libjars     /srv/deployment/analytics/refinery/artifacts/wikihadoop.jar \
    -libjars     ~/TreeExtractor.jar
    -D           mapreduce.output.fileoutputformat.compress=false \
    -D           mapreduce.input.fileinputformat.split.minsize=300000000 \
    -D           mapreduce.task.timeout=6000000 \
    #-files       cat.py \
    #-archives    'hdfs:///user/west1/virtualenv.zip#virtualenv' \
    -inputformat SequenceFileAsTextInputFormat \
    -input       /wmf/data/raw/webrequest/webrequest_text/hourly/2014/12/04/01/webrequest_text.22.8.* \
    -output      "/user/west1/tree_extractor_test" \
    -mapper      org.wikimedia.west1.traces.GroupByUserAndDayMapper \
    -reducer     org.wikimedia.west1.traces.TreeExtractorReducer
