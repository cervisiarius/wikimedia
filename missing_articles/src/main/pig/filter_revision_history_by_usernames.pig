/*                                                                                               
Set language, e.g.,
pig \
-param PARALLEL=100 \
filter_revision_history_by_usernames.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

--Rev = LOAD '/tmp/rev.tsv' USING PigStorage('\t')
Rev = LOAD '/user/west1/revision_history/en' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray,
        minor:chararray, deleted:chararray, length:int, parent_id:int, comment:chararray);

-- Keep only edits made by logged-in users.
Rev = FILTER Rev BY (user_id > 0);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
    page_id,
    user,
    timestamp,
    length;

-- Keep only desired users.
DEFINE hashJoin `./hash_join.pl RfA_unique_users.txt 2 1` ship('/home/west1/wikimedia/trunk/missing_articles/src/main/perl/hash_join.pl', '/home/west1/wikimedia/trunk/data/RfA_unique_users.txt');
Rev = STREAM Rev THROUGH hashJoin AS (page_id:int, user:chararray, timestamp:chararray, length:int);

-- Make sure everything by the same user appears sequentially.
SortedByUser = ORDER Rev BY user, timestamp PARALLEL $PARALLEL;

--STORE SortedByUser INTO '/tmp/revision_history_RfA-ONLY';
STORE SortedByUser INTO '/user/west1/revision_history_RfA-ONLY';
