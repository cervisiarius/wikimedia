SET mapreduce.output.fileoutputformat.compress false;

---------------------------------------------------------------------------------------------------
-- Revisions
---------------------------------------------------------------------------------------------------

-- Load the pagecount data.
-- NB: the '-tagfile' flag seems to be buggy and throw the indices off: Both the filename and the
-- domain field contain the file name in that case.
Rev = LOAD '/user/west1/revision_history/af' USING PigStorage('\t')
	AS (id:int, text_id:int, user_id:int, user:chararray, timestamp:chararray, minor:chararray,
		....................);



a.rev_id,
      a.rev_page,
      a.rev_text_id,
      a.rev_user,
      CAST(a.rev_user_text AS CHAR(255) CHARSET utf8) AS rev_user_text,
      CAST(a.rev_timestamp AS CHAR(255) CHARSET utf8) AS rev_timestamp,
      a.rev_minor_edit,
      a.rev_deleted,
      a.rev_len,
      a.rev_parent_id,
      CAST(a.rev_comment AS CHAR(255) CHARSET utf8) AS rev_comment


-- Keep only entries corresponding to a Wikipedia domain in any language; since all domains except
-- Wikipedia contain a dot in the name (e.g. "en.d"), we simply look for domains without a dot.
Counts = FILTER Counts BY (INDEXOF(domain, '.', 0) < 0) AND page_title IS NOT NULL;

-- Remove the unnecessary columns, lower-case the language code, and URL-decode all article names.
-- Also replace spaces with underscores.
Counts = FOREACH Counts GENERATE
	LOWER(domain) AS lang,
	REPLACE(org.wikimedia.west1.pigudf.URLDECODE(page_title), ' ', '_') AS page_title,
	count_views;

-- Add the key for joining with redirects.
Counts = FOREACH Counts GENERATE
	CONCAT(CONCAT(lang, ' '), page_title) AS key,
	lang,
	page_title,
	count_views;


---------------------------------------------------------------------------------------------------
-- Titles
---------------------------------------------------------------------------------------------------

----


---------------------------------------------------------------------------------------------------
-- Wikidata
---------------------------------------------------------------------------------------------------

-- Read the Wikidata file that maps Wikidata entries to Wikipedia articles.
Wikidata = LOAD '/user/west1/interlanguage_links.tsv' USING PigStorage('\t')
	AS (mid:chararray, lang:chararray, page_title:chararray);

-- Add the key for joining with pagecounts and replace spaces with underscores in titles.
Wikidata = FOREACH  Wikidata GENERATE
	CONCAT(CONCAT(lang, ' '), REPLACE(page_title, ' ', '_')) AS key,
	mid,
	lang,
	REPLACE(page_title, ' ', '_') AS page_title;


---------------------------------------------------------------------------------------------------
-- Join the three tables
---------------------------------------------------------------------------------------------------

-- Join pagecounts and redirects.
CRJoined = JOIN Counts BY key LEFT OUTER, Redir BY key;

-- Remove unnecessary columns. Also regenerate the keys to reflect redirects.
CRJoined = FOREACH CRJoined GENERATE
	CONCAT(CONCAT(Counts::lang, ' '), (Redir::tgt IS NOT NULL ? Redir::tgt : Counts::page_title)) AS key,
	Counts::lang AS lang,
	(Redir::tgt IS NOT NULL ? Redir::tgt : Counts::page_title) AS page_title,
	Counts::count_views AS count_views;

-- Sum counts for the same article across all days.
CRAggr = GROUP CRJoined BY key;
CRAggr = FOREACH CRAggr GENERATE
	group AS key,
	MIN(CRJoined.lang) AS lang,
	MIN(CRJoined.page_title) AS page_title,
	SUM(CRJoined.count_views) AS count_views;

-- Join WikiData and CRAggr.
CRWJoined = JOIN CRAggr BY key LEFT OUTER, Wikidata BY key;

CRWJoined = FOREACH CRWJoined GENERATE
	Wikidata::mid AS mid,
	CRAggr::lang AS lang,
	CRAggr::page_title AS page_title,
	CRAggr::count_views AS count_views;

STORE CRWJoined INTO '/user/west1/pagecounts';
