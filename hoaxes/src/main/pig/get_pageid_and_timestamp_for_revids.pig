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
Rev = LOAD '/user/west1/enwiki_revisions_with_page_titles' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, page_title:chararray, user_id:int, user:chararray, timestamp:chararray,
        length:int, parent_id:int);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    rev_id,
    page_id,
    page_title,
    timestamp;


---------------------------------------------------------------------------------------------------
-- List of relevant revids
---------------------------------------------------------------------------------------------------

--RevId = LOAD '/user/west1/srijans_revids' USING PigStorage('\t')
RevId = LOAD '/tmp/srijans_revids.tsv' USING PigStorage('\t')
    AS (rev_id:long);


---------------------------------------------------------------------------------------------------
-- Join the two tables
---------------------------------------------------------------------------------------------------

-- Add titles to revisions.
Joined = JOIN Rev BY rev_id, RevId BY rev_id PARALLEL $PARALLEL;
Joined = FOREACH Joined GENERATE 
    Rev::rev_id AS rev_id,
    Rev::page_id AS page_id,
    Rev::page_title AS page_title,
    Rev::timestamp AS timestamp;

STORE Joined INTO '/user/west1/srijans_revids_with_pageids_and_timestamps';
