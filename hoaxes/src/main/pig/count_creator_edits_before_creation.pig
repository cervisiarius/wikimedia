/*
pig \
-param PARALLEL=100 \
count_creator_edits_before_creation.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

-- Load first-revision info (only consider logged-in users, i.e., user_id > 0).
First = LOAD '/user/west1/enwiki_metadata/enwiki_first_revision_per_page' USING PigStorage('\t')
    AS (page_id:int, timestamp:chararray, rev_id:long, page_title:chararray, user_id:int, user:chararray);
First = FILTER First BY user_id > 0;

-- Load the revision data (only consider logged-in users, i.e., user_id > 0).
Rev = LOAD '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles.tsv' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, page_title:chararray, user_id:int, user:chararray, timestamp:chararray,
        length:int, parent_id:int);
Rev = FILTER Rev BY user_id > 0;

-- Find previous edits by the creator of each page.
Joined = JOIN Rev BY user_id, First BY user_id USING 'replicated' PARALLEL $PARALLEL;
Joined = FILTER Joined BY (Rev::timestamp < First::timestamp);
Joined = FOREACH Joined GENERATE
    First::page_id AS page_id,
    First::timestamp AS timestamp,
    First::rev_id AS rev_id,
    First::page_title AS page_title,
    First::user_id AS user_id,
    First::user AS user;

-- Count previous edits.
Grouped = GROUP Joined BY page_id PARALLEL $PARALLEL;
Grouped = FOREACH Grouped GENERATE
    MIN(Joined.page_id) AS page_id,
    MIN(Joined.timestamp) AS timestamp,
    MIN(Joined.rev_id) AS rev_id,
    MIN(Joined.page_title) AS page_title,
    MIN(Joined.user_id) AS user_id,
    MIN(Joined.user) AS user,
    COUNT(Joined) AS num_prior_edits;

STORE Grouped INTO '/user/west1/enwiki_metadata/enwiki_first_revision_per_page_WITH-NUM-PRIOR-EDITS';
