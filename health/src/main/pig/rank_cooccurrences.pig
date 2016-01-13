/*
pig \
-param PARALLEL=200 \
-param LANG=en \
rank_cooccurrences.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

%declare TARGET_PAGE 'Electronic_cigarette'

--Baskets = LOAD '/tmp/sample.txt' USING PigStorage('\t') AS (uid:chararray, basket:chararray);

Baskets = LOAD '/user/west1/user_baskets/$LANG' USING PigStorage('\t')
  AS (uid:chararray, basket:chararray);

-- Keep only the users who viewed the target page.
Baskets = FILTER Baskets BY basket MATCHES '(^|.*\\|)$TARGET_PAGE(\\|.*|$)';

-- Flatten the baskets. A page will now appear n times if n users have visited it.
Flat = FOREACH Baskets GENERATE FLATTEN(TOKENIZE(basket, '|')) AS page;

-- Count for each page how many users have visited it.
Grouped = GROUP Flat BY page PARALLEL $PARALLEL;
Counts = FOREACH Grouped GENERATE group AS page, COUNT(Flat.page) AS count;

Counts = ORDER Counts BY count DESC;

STORE Counts INTO '/user/west1/health/cooccurrences_with_$TARGET_PAGE/$LANG';
