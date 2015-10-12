/*                                                                                               
pig \
-param PARALLEL=10 \
find_first_revision_per_page.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

-- Load the pagecount data.
Rev = LOAD '/user/west1/revision_history/en' USING PigStorage('\t')
--Rev = LOAD '/tmp/revision_history.tsv' USING PigStorage('\t')
	AS (rev_id:chararray, page_id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray,
        minor:chararray, deleted:chararray, length:int, parent_id:int, comment:chararray);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    rev_id,
    page_id,
    timestamp,
    CONCAT(CONCAT(timestamp, '\t'), rev_id) AS time_rev;

Grouped = GROUP Rev BY page_id PARALLEL $PARALLEL;
Grouped = FOREACH Grouped GENERATE
    MIN(Rev.page_id) AS page_id,
    MIN(Rev.time_rev) AS time_rev;

STORE Grouped INTO '/user/west1/enwiki_first_revision_per_page';
--STORE Grouped INTO '/tmp/enwiki_first_revision_per_page';
