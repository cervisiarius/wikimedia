#!/bin/bash

# Modify these parameters.
# This is where additional JARs reside.
export LIB_DIR=$HOME/wikimedia/trunk/lib
# The part of the server logs you want to process.
export IN_DIR=/user/west1/webrequest_source=text/year=2015/month=2/*/*/*
# The output directory.
export OUT_DIR=/user/west1/webrequest_source=text__SPARSE/year=2015/month=2/*/*/*

echo "Running hadoop job"
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars      $LIB_DIR/iow-hadoop-streaming-1.0.jar \
    -D            mapreduce.output.fileoutputformat.compress=true \
    -D            mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
    -D            parquet.read.support.class=net.iponweb.hadoop.streaming.parquet.GroupReadSupport \
    -D            parquet.read.schema="message webrequest_schema {
                                        optional binary hostname;
                                        optional int64 sequence;
                                        optional binary dt; 
                                        optional binary ip;
                                        optional binary http_status;
                                        optional binary http_method;
                                        optional binary uri_host;
                                        optional binary uri_path;
                                        optional binary uri_query;
                                        optional binary content_type;
                                        optional binary referer;
                                        optional binary x_forwarded_for;
                                        optional binary user_agent;
                                        optional binary accept_language;
                                        optional binary record_version;
                                        optional binary geocoded_data;
                                        optional int32 year;
                                        optional int32 month;
                                        optional int32 day;
                                        optional int32 hour;
                                       }" \
    -D            mapreduce.task.timeout=6000000 \
    -D            mapreduce.map.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.map.output.value.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.key.class=org.apache.hadoop.io.Text \
    -D            mapreduce.job.output.value.class=org.apache.hadoop.io.Text \
    -inputformat  net.iponweb.hadoop.streaming.parquet.ParquetAsJsonInputFormat \
    -outputformat net.iponweb.hadoop.streaming.parquet.ParquetAsJsonOutputFormat \
    -input        $IN_DIR \
    -output       $OUT_DIR \
    -mapper       "/bin/cat" \
    -reducer      "/bin/cat"
#    -numReduceTasks 0
