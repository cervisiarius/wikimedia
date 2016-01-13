/*
pig \
-param PARALLEL=200 \
-param LANG=en \
rank_cooccurrences.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

%declare TARGET_PAGE 'Electronic_cigarette'
-- Set to '_DAILY' if you want to consider the baskets that don't span more than a day.
%declare DAILY_SUFFIX '_DAILY'

--Baskets = LOAD '/tmp/sample.txt' USING PigStorage('\t') AS (uid:chararray, basket:chararray);

Baskets = LOAD '/user/west1/user_baskets$DAILY_SUFFIX/$LANG' USING PigStorage('\t')
  AS (id:chararray, basket:chararray);

-- Discard uid.
Baskets = FOREACH Baskets GENERATE basket;

-- Flatten the baskets. A page will now appear n times if n users have visited it. Then count.
Flat = FOREACH Baskets GENERATE FLATTEN(TOKENIZE(basket, '|')) AS page;
Grouped = GROUP Flat BY page PARALLEL $PARALLEL;
Counts = FOREACH Grouped GENERATE group AS page, COUNT(Flat.page) AS count;

-- Same for only the baskets that contain the target page. Consider only those that appear at
-- least 10 times together with the target page.
MatchingBaskets = FILTER Baskets BY basket MATCHES '(^|.*\\|)$TARGET_PAGE(\\|.*|$)';
MatchingFlat = FOREACH MatchingBaskets GENERATE FLATTEN(TOKENIZE(basket, '|')) AS page;
MatchingGrouped = GROUP MatchingFlat BY page PARALLEL $PARALLEL;
MatchingCounts = FOREACH MatchingGrouped GENERATE group AS page, COUNT(MatchingFlat.page) AS count;
MatchingCounts = FILTER MatchingCounts BY count >= 10;

-- Normalize. NB: large table must come first in replicated join.
Normalized = JOIN Counts BY page, MatchingCounts BY page USING 'replicated';
Normalized = FOREACH Normalized GENERATE
  Counts::page AS page,
  Counts::count AS single_count,
  MatchingCounts::count AS pair_count,
  ((double) MatchingCounts::count / Counts::count) AS ratio:double;
Normalized = ORDER Normalized BY ratio DESC;

STORE Normalized INTO '/user/west1/health/cooccurrences_with_$TARGET_PAGE$DAILY_SUFFIX/$LANG';
