/*
pig \
-param PARALLEL=200 \
trees_to_baskets.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

Trees = LOAD '/user/west1/navigation_trees/year=2015/month=12/en/part-r-00043' USING PigStorage('\t')
  AS (json:chararray);

--Trees = LOAD '/tmp/sample.txt' USING PigStorage('\t') AS (json:chararray);

-- Extract the list of pages that make up a tree.
Data = FOREACH Trees GENERATE
  REPLACE(json, '.*?\\"id\\":\\"(.+?)_\\d+_\\d+\\".*', '$1') AS uid,
  REPLACE(REPLACE(json, '.*?\\"title\\":\\"(.*?)\\"', '$1|'), '(.*)\\|.*', '$1') AS page_list;

-- Produce one row per user, with all viewed pages concatenated by '|'; this list may contain
-- duplicates.
Grouped = GROUP Data BY uid PARALLEL $PARALLEL;
Baskets = FOREACH Grouped GENERATE group AS uid, BagToString(Data.page_list, '|') AS page_list;

-- Remove the duplicates in each user's basket.
DEFINE uniq `perl -ne 'chomp; ($uid, $pages) = split /\\t/; %hash = map { $_ => 1 } (split /\\|/, $pages); print $uid . "\t" . join("|", keys %hash) . "\n";'` input(stdin using PigStreaming('\t')) output (stdout using PigStreaming('\t'));
Baskets = STREAM Baskets THROUGH uniq AS (uid:chararray, page_set:chararray);

STORE Baskets INTO '/user/west1/health/__TEST__';
