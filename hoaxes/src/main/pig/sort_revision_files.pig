/*
pig \
-param PARALLEL=200 \
sort_revision_files.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

-- Load first-revision info (only consider logged-in users, i.e., user_id > 0).
First = LOAD '/user/west1/enwiki_metadata/enwiki_first_revision_per_page' USING PigStorage('\t')
    AS (page_id:int, timestamp:chararray, rev_id:long, page_title:chararray, user_id:int, user:chararray);
First = FILTER First BY user_id > 0;

-- Sort.
--First = ORDER First BY user_id, timestamp PARALLEL $PARALLEL;

-- Load the revision data (only consider logged-in users, i.e., user_id > 0).
Rev = LOAD '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles.tsv' USING PigStorage('\t')
    AS (rev_id:long, page_id:int, page_title:chararray, user_id:int, user:chararray, timestamp:chararray,
        length:int, parent_id:int);
Rev = FILTER Rev BY user_id > 0;

-- Sort.
--Rev = ORDER Rev BY user_id, timestamp PARALLEL $PARALLEL;

Joined = JOIN Rev BY rev_id LEFT OUTER, First BY rev_id PARALLEL $PARALLEL;
Joined = FOREACH Joined GENERATE
    Rev::rev_id, Rev::page_id, Rev::page_title, Rev::user_id, Rev::user, Rev::timestamp,
    (First::rev_id is null ? 0 : 1) AS is_first;

Sorted = ORDER Joined BY user_id, timestamp PARALLEL $PARALLEL;

STORE Sorted INTO '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles_WITH-FIRST-FLAG_SORTED-BY-UID+TIME';

--STORE First INTO '/user/west1/enwiki_metadata/enwiki_first_revision_per_page_SORTED-BY-UID+TIME';
--STORE Rev INTO '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles_SORTED-BY-UID+TIME';
