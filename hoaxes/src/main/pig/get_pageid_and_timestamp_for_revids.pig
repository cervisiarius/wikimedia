/*
pig \
-param PARALLEL=10 \
get_pageid_and_timestamp_for_revids.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

-- Load the revision data.
Rev = LOAD '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles.tsv' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, page_title:chararray, user_id:int, user:chararray, timestamp:chararray,
        length:int, parent_id:int);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    rev_id,
    page_id,
    user,
    timestamp;


---------------------------------------------------------------------------------------------------
-- List of relevant revids
---------------------------------------------------------------------------------------------------

--RevId = LOAD '/user/west1/hoaxes/srijans_revids.tsv' USING PigStorage('\t')
--    AS (rev_id:long);

RevId = LOAD '/user/srijan/article_mention_impact_and_speedy/' USING PigStorage('\t')
    AS (mention:chararray, rev_id:long);

---------------------------------------------------------------------------------------------------
-- Join the two tables
---------------------------------------------------------------------------------------------------

-- Add titles to revisions.
Joined = JOIN Rev BY rev_id, RevId BY rev_id PARALLEL $PARALLEL;
Joined = FOREACH Joined GENERATE 
    Rev::rev_id AS rev_id,
    Rev::page_id AS page_id,
    RevId::mention AS mention,
    Rev::user AS user,
    Rev::timestamp AS timestamp;

-- STORE Joined INTO '/user/west1/hoaxes/srijans_revids_with_pageids_and_timestamps';
STORE Joined INTO '/user/west1/hoaxes/article_mention_impact_and_speedy_WITH-ADDITIONAL-DATA';
