/*
pig \
-param PARALLEL=100 \
sort_revision_table_and_add_first_flag.pig
*/

-- NB: Output to be processed by count_creator_edits_before_creation.py.

SET mapreduce.output.fileoutputformat.compress false;

-- Load first-revision info.
First = LOAD '/user/west1/enwiki_metadata/enwiki_first_and_last_revision_per_page' USING PigStorage('\t')
    AS (page_id:int,
        timestamp:chararray, rev_id:long, page_title:chararray, user_id:int, user:chararray,
        last_timestamp:chararray, last_rev_id:long, last_page_title:chararray, last_user_id:int, last_user:chararray);
First = FOREACH First GENERATE rev_id;
-- Only consider logged-in users, i.e., user_id > 0.
--First = FILTER First BY user_id > 0;

-- Load the revision data.
Rev = LOAD '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles.tsv' USING PigStorage('\t')
    AS (rev_id:long, page_id:int, page_title:chararray, user_id:int, user:chararray, timestamp:chararray,
        length:int, parent_id:int);
-- Only consider logged-in users, i.e., user_id > 0.
--Rev = FILTER Rev BY user_id > 0;

Joined = JOIN Rev BY rev_id LEFT OUTER, First BY rev_id PARALLEL $PARALLEL;
Joined = FOREACH Joined GENERATE
    Rev::rev_id, Rev::page_id, Rev::page_title, Rev::user_id, Rev::user, Rev::timestamp,
    (First::rev_id is null ? 0 : 1) AS is_first;

Sorted = ORDER Joined BY user, timestamp PARALLEL $PARALLEL;

STORE Sorted INTO '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles_WITH-FIRST-FLAG_SORTED-BY-USER+TIME';
