/*
pig \
-param PARALLEL=10 \
add_page_title_to_revision_history.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

-- Load the pagecount data.
Rev = LOAD '/user/west1/revision_history/en' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray,
        minor:chararray, deleted:chararray, length:int, parent_id:int, comment:chararray);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    rev_id,
    page_id,
    user_id,
    user,
    timestamp,
    length,
    parent_id;


---------------------------------------------------------------------------------------------------
-- Titles
---------------------------------------------------------------------------------------------------

Pages = LOAD '/user/west1/pages/en' USING PigStorage('\t')
--Pages = LOAD '/tmp/pages.tsv' USING PigStorage('\t')
    AS (page_id:int, page_title:chararray, is_redirect:chararray);


---------------------------------------------------------------------------------------------------
-- Join the two tables
---------------------------------------------------------------------------------------------------

-- Add titles to revisions.
RPJoined = JOIN Rev BY page_id, Pages BY page_id PARALLEL $PARALLEL;
RPJoined = FOREACH RPJoined GENERATE 
    Rev::rev_id AS rev_id,
    Rev::page_id AS page_id,
    Pages::page_title AS page_title,
    Rev::user_id AS user_id,
    Rev::user AS user,
    Rev::timestamp AS timestamp,
    Rev::length AS length,
    Rev::parent_id AS parent_id;

-- Make sure everything for the same page appears sequentially.
SortedByPage = ORDER RPJoined BY page_id, rev_id PARALLEL $PARALLEL;

STORE SortedByPage INTO '/user/west1/enwiki_revisions_with_page_titles';
