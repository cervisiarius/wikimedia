-- NB: UNUSED

--$page_id, $page_title, $page_redirectTitle, $page_isRedirect, $revision_id, $revision_parentid,
--$revision_timestamp, $revision_userid, $revision_username, $revision_length, $revision_links_string

-- NB: misnomer: it's really the link history, not the deltas (Ashwin mixed up the names).
edits = LOAD '/user/ashwinp/revision_dump_deltas_20150403/*' USING PigStorage('\t')
--edits = LOAD '/tmp/pig.txt' USING PigStorage('\t')
	AS (id:int, page_title:chararray, x1:chararray, x2:chararray, x3:chararray, x4:chararray,
		date:chararray, x5:chararray, x6:chararray, x7:chararray, links:chararray);

edits = FOREACH edits GENERATE
	REPLACE(page_title, ' ', '_') AS page_title,
	--SecondsBetween(ToDate(date, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC'), ToDate('2014-01-01T00:00:00Z', 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC')) AS early_diff,
	--SecondsBetween(ToDate(date, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC'), ToDate('2015-01-01T00:00:00Z', 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC')) AS late_diff,
	date,
	CONCAT(date, CONCAT('\t', REPLACE(links, ' ', '_'))) AS date_links;

--edits_1 = FILTER edits BY early_diff < 0;
edits_1 = FILTER edits BY (date MATCHES '^2014-.*');
groups_1 = GROUP edits_1 BY page_title;
top_1 = FOREACH groups_1 GENERATE group AS page_title, MAX(edits_1.date_links) AS date_links;

STORE top_1 INTO '/user/west1/_top_1';

--edits_2 = FILTER edits BY late_diff > 0;
edits_2 = FILTER edits BY (date MATCHES '^2015-0[12].*');
groups_2 = GROUP edits_2 BY page_title;
top_2 = FOREACH groups_2 GENERATE group AS page_title, MAX(edits_2.date_links) AS date_links;

STORE top_2 INTO '/user/west1/_top_2';

top = JOIN top_1 BY page_title, top_2 BY page_title;

top = FOREACH top GENERATE
	top_1::page_title,
	top_1::date_links,
	top_2::date_links;

STORE top INTO '/user/west1/early_and_late_versions_of_all_articles';
