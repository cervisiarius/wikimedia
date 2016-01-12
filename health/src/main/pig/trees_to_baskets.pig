/*
pig \
-param PARALLEL=200 \
trees_to_baskets.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

Trees = LOAD '/user/west1/navigation_trees/year=2015/month=12/en/part-r-00043' USING PigStorage('\t')
    AS (json:chararray);

Data = FOREACH Trees GENERATE REPLACE(json, '.*?\\"title\\":\\"(.*?)\\"', '\1') AS page_list;

STORE Data INTO '/user/west1/health/__TEST__';
