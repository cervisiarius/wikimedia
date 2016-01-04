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
--Rev = LOAD '/user/west1/revision_history/en' USING PigStorage('\t')
--	AS (rev_id:chararray, page_id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray,
--        minor:chararray, deleted:chararray, length:int, parent_id:int, comment:chararray);

Rev = LOAD '/user/west1/enwiki_metadata/enwiki_revisions_with_page_titles.tsv' USING PigStorage('\t')
    AS (rev_id:chararray, page_id:int, page_title:chararray, user_id:chararray, user:chararray, timestamp:chararray,
        length:int, parent_id:int);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    --rev_id,
    --timestamp,
    page_id,
    CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(
        timestamp, '\t'), rev_id), '\t'), page_title), '\t'), user_id), '\t'), user) AS rev_info;

Grouped = GROUP Rev BY page_id PARALLEL $PARALLEL;
Grouped = FOREACH Grouped GENERATE
    MIN(Rev.page_id) AS page_id,
    MIN(Rev.rev_info) AS rev_info;

STORE Grouped INTO '/user/west1/enwiki_metadata/enwiki_first_revision_per_page';
--STORE Grouped INTO '/tmp/enwiki_first_revision_per_page';
