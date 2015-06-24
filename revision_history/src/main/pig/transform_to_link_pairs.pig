SET mapreduce.output.fileoutputformat.compress false;
Register 'split_to_bag.py' using jython as pipe_splitter;
link_deltas = LOAD 'hdfs:///user/ashwinp/revision_dump_link_deltas_20150403/*' AS (page_id:int ,page_title:chararray, revision_id:int, revision_parentid:int, revision_timestamp:datetime, revision_userid:int, revision_username:chararray, revision_length:int, deleted_links:chararray, added_links:chararray);

--link_deltas = LOAD '/dfs/scratch1/ashwinp/revision_dump_link_deltas_20150403/enwiki-20150403-pages-meta-history9.xml-p000892336p000925000' AS (page_id:int ,page_title:chararray, revision_id:int, revision_parentid:int, revision_timestamp:datetime, revision_userid:int, revision_username:chararray, revision_length:int, deleted_links:chararray, added_links:chararray);
link_deltas = FILTER link_deltas BY revision_timestamp is not null;

filtered_link_deltas = FILTER link_deltas BY SecondsBetween(revision_timestamp, (datetime)ToDate('2015-01-01T00:00:00Z')) >= (long)0;

added_st_pairs = FOREACH filtered_link_deltas GENERATE page_id .. revision_length, FLATTEN(pipe_splitter.splitToBag(added_links)) as target, 1 as change;
deleted_st_pairs = FOREACH filtered_link_deltas GENERATE page_id .. revision_length, FLATTEN(pipe_splitter.splitToBag(deleted_links)) as target, -1 as change;
flattened_st_pairs = UNION added_st_pairs, deleted_st_pairs;

st_pairs_grouped = GROUP flattened_st_pairs BY (page_id, page_title, target);
temp = FOREACH st_pairs_grouped GENERATE FLATTEN(flattened_st_pairs);
--STORE temp INTO 'before_filtering';
st_pairs_single_change = FILTER st_pairs_grouped BY COUNT(flattened_st_pairs)==1;
st_pairs_single_change = FOREACH st_pairs_single_change GENERATE FLATTEN(flattened_st_pairs);

st_pairs_useful = FILTER st_pairs_single_change BY change==1 AND SecondsBetween(revision_timestamp, (datetime)ToDate('2015-02-01T00:00:00Z')) >= (long)0 AND SecondsBetween((datetime)ToDate('2015-03-01T00:00:00Z'), revision_timestamp) >= (long)0;
STORE st_pairs_useful INTO 'useful_st_pairs';

