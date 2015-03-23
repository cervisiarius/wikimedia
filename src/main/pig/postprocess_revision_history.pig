/*                                                                                               
Set language, e.g.,
pig \
-param LANG=ca \
-param PARALLEL=10 \
postprocess_revision_history.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

-- Load the pagecount data.
-- NB: the '-tagfile' flag seems to be buggy and throw the indices off: Both the filename and the
-- domain field contain the file name in that case.
--Rev = LOAD '/tmp/rev.tsv' USING PigStorage('\t')
Rev = LOAD '/user/west1/revision_history/$LANG' USING PigStorage('\t')
	AS (rev_id:long, page_id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray,
        minor:chararray, deleted:chararray, length:int, parent_id:int, comment:chararray);

-- Keep only edits made by logged-in users.
Rev = FILTER Rev BY (user_id > 0);

-- Remove the unnecessary columns.
Rev = FOREACH Rev GENERATE
	rev_id,
    page_id,
    user_id,
    user,
    timestamp,
    length,
    parent_id;

-- Remove bots.
DEFINE hashUnjoin `./hash_unjoin.pl bots.txt 4 1` ship('/home/west1/wikimedia/trunk/src/main/perl/hash_unjoin.pl', '/home/west1/wikimedia/trunk/data/bots/bots.txt');
Rev = STREAM Rev THROUGH hashUnjoin AS (rev_id:long, page_id:int, user_id:int, user:chararray,
        timestamp:chararray, length:int, parent_id:int);


---------------------------------------------------------------------------------------------------
-- Titles
---------------------------------------------------------------------------------------------------

Pages = LOAD '/user/west1/pages/$LANG' USING PigStorage('\t')
--Pages = LOAD '/tmp/pages.tsv' USING PigStorage('\t')
    AS (page_id:int, page_title:chararray, is_redirect:chararray);

-- Discard redirects.
Pages = FILTER Pages BY (is_redirect == 'false');


---------------------------------------------------------------------------------------------------
-- Wikidata
---------------------------------------------------------------------------------------------------

-- Read the Wikidata file that maps Wikidata entries to Wikipedia articles.
--Wikidata = LOAD '/tmp/interlanguage_links.tsv' USING PigStorage('\t')
Wikidata = LOAD '/user/west1/interlanguage_links.tsv' USING PigStorage('\t')
	AS (mid:chararray, lang:chararray, page_title:chararray);

-- Keep only the language of interest.
Wikidata = FILTER Wikidata BY (lang == '$LANG');

-- Replace spaces with underscores in titles.
Wikidata = FOREACH  Wikidata GENERATE
	mid,
	REPLACE(page_title, ' ', '_') AS page_title;


---------------------------------------------------------------------------------------------------
-- Join the three tables
---------------------------------------------------------------------------------------------------

-- Add titles to revisions.
RPJoined = JOIN Rev BY page_id, Pages BY page_id;
RPJoined = FOREACH RPJoined GENERATE 
    Rev::rev_id AS rev_id,
    Pages::page_title AS page_title,
    Rev::user_id AS user_id,
    Rev::user AS user,
    Rev::timestamp AS timestamp,
    Rev::length AS length,
    Rev::parent_id AS parent_id;

-- Add Wikidata ids.
RPWJoined = JOIN RPJoined BY page_title, Wikidata BY page_title;
RPWJoined = FOREACH RPWJoined GENERATE 
    RPJoined::rev_id AS rev_id,
    Wikidata::mid AS mid,
    RPJoined::page_title AS page_title,
    RPJoined::user_id AS user_id,
    RPJoined::user AS user,
    RPJoined::timestamp AS timestamp,
    RPJoined::length AS length,
    RPJoined::parent_id AS parent_id;

-- Join against revisions again, in order to compute byte difference.
RPWPJoined = JOIN RPWJoined BY parent_id LEFT OUTER, Rev BY rev_id;
RPWPJoined = FOREACH RPWPJoined GENERATE 
    RPWJoined::mid AS mid,
    RPWJoined::page_title AS page_title,
    RPWJoined::user_id AS user_id,
    RPWJoined::user AS user,
    RPWJoined::timestamp AS timestamp,
    (Rev::rev_id IS NULL ? RPWJoined::length :
        (RPWJoined::length - Rev::length >= 0 ? RPWJoined::length - Rev::length : 0)) AS bytes_added;


---------------------------------------------------------------------------------------------------
-- Group
---------------------------------------------------------------------------------------------------

-- Aggregate by user/mid.
-- TODO: Compute number of days on which editors was active on the article, and the number of days
-- between the first and last edits.
GroupedByPair = GROUP RPWPJoined BY (user_id, mid);
GroupedByPair = FOREACH GroupedByPair GENERATE
    MIN(RPWPJoined.user_id) AS user_id,
    MIN(RPWPJoined.user) AS user,
    MIN(RPWPJoined.mid) AS mid,
    MIN(RPWPJoined.page_title) AS page_title,
	COUNT(RPWPJoined) AS num_edits,
	SUM(RPWPJoined.bytes_added) AS bytes_added;

-- Make sure everything by the same user appears sequentially.
SortedByUser = ORDER GroupedByPair BY user, mid PARALLEL $PARALLEL;

STORE Grouped INTO '/user/west1/revision_history_aggregated/$LANG';
