set output.compression.enabled false;

--DEFINE UrlDecode InvokeForString('java.net.URLDecoder.decode', 'String String'); 
REGISTER /home/west1/wikimedia/trunk/src/main/pig/udf/pigudf.jar;

-- Load the pagecount data.
Counts = LOAD '/wmf/data/archive/pagecounts-all-sites/*/*/*.gz' USING PigStorage(' ', '-tagFile')
	AS (filename:chararray, domain:chararray, page_title:chararray,	count_views:long, total_response_size:chararray);

--Counts = LIMIT Counts 100;

-- Keep only entries corresponding to a Wikipedia domain in any language.
Counts = FILTER Counts BY (INDEXOF(domain, '.', 0) < 0) AND page_title IS NOT NULL;

-- Remove the unnecessary columns, lower-case the language code, and URL-decode all article names.
Counts = FOREACH Counts GENERATE
	LOWER(domain) AS lang,
	org.wikimedia.west1.pigudf.URLDECODE(page_title) AS page_title,
	count_views;

-- Add the key for joining with redirects.
Counts = FOREACH Counts GENERATE
	CONCAT(CONCAT(lang, ' '), page_title) AS key,
	lang,
	page_title,
	count_views;

-- Load the redirects.
Redir = LOAD '/user/west1/redirects/aggregated/redirects_for_all_languages.tsv' USING PigStorage('\t')
	AS (lang:chararray, src:chararray, tgt:chararray);

-- Add the key for joining with redirects.
Redir = FOREACH Redir GENERATE
	CONCAT(CONCAT(lang, ' '), src) AS key,
	lang,
	src,
	tgt;

-- Join pagecounts and redirects.
CRJoined = JOIN Counts BY key LEFT OUTER, Redir BY key;

-- Remove unnecessary columns.
CRJoined = FOREACH CRJoined GENERATE
	Counts::key AS key,
	Counts::lang AS lang,
	(Redir::tgt IS NOT NULL ? Redir::tgt : Counts::page_title) AS page_title,
	Counts::count_views AS count_views;

-- Sum counts for the same article across all days.
CRAggr = GROUP CRJoined BY key;
CRAggr = FOREACH  CRAggr GENERATE
	group AS key,
	MIN(CRJoined.lang) AS lang,
	MIN(CRJoined.page_title) AS page_title,
	SUM(CRJoined.count_views) AS count_views;

-- Read the Wikidata file that maps Wikidata entries to Wikipedia articles.
Wikidata = LOAD '/user/west1/interlanguage_links.tsv'
	AS (mid:chararray, lang:chararray, page_title:chararray);

-- Add the key for joining with pagecounts.
Wikidata = FOREACH  Wikidata GENERATE
	CONCAT(CONCAT(lang, ' '), page_title) AS key,
	mid,
	lang,
	page_title;

-- Join WikiData and CRAggr.
Joined = JOIN CRAggr BY key LEFT OUTER, Wikidata BY key;

Joined = FOREACH Joined GENERATE
	Wikidata::mid AS mid,
	CRAggr::lang AS lang,
	CRAggr::page_title AS page_title,
	CRAggr::count_views AS count_views;

STORE Joined INTO '/user/west1/pagecounts';
