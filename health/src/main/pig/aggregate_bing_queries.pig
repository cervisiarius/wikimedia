/*
pig \
-param PARALLEL=200 \
aggregate_bing_queries.pig
*/

%declare TITLE_REGEX (Tobacco|Electronic_cigarette|Lung_cancer|Smoking_cessation|Nicotine|Influenza|Influenza_vaccine)

SET mapreduce.output.fileoutputformat.compress false;

Trees = LOAD '/user/west1/navigation_trees/year=2015/month=1[12]/en/' USING PigStorage('\t')
    AS (json:chararray);

-- Keep only trees that have a matching page in the root (by first removing children), and whose
-- referer is a Bing SERP.
Trees = FILTER Trees BY
  REPLACE(json, '\\"children\\":\\[.*\\],?', '') MATCHES '.*\\"title\\":\\"$TITLE_REGEX\\".*'
  AND json MATCHES '.*\\"referer\\":\\"https?://([^/]+\\.)?bing\\.com/search\\?.*?q=.*?\\".*';

Data = FOREACH Trees GENERATE
  REPLACE(json, '.*?\\"id\\":\\"(.+?)_\\d+_\\d+\\".*', '$1') AS uid,
  REPLACE(REPLACE(json, '\\"children\\":\\[.*\\],?', ''), '.*?\\"title\\":\\"(.*?)".*', '$1') AS title,
  REPLACE(REPLACE(json, '.*\\"referer\\":\\"https?://([^/]+\\.)?bing\\.com/search\\?[^"]*?q=([^"]*?)(&|").*', '$2'), '\\+', ' ') AS query;

-- Discard multiple identical queries by the same user.
Data = DISTINCT Data;

-- Aggregate by country and state.
Grouped = GROUP Data BY (title, query) PARALLEL $PARALLEL;
Counts = FOREACH Grouped GENERATE
    group.title AS title,
    group.query AS query,
    COUNT(Data) AS count;

Counts = ORDER Counts BY title, count DESC;

STORE Counts INTO '/user/west1/health/bing_query_counts';
