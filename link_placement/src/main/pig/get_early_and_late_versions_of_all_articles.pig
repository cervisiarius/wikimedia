--$page_id, $page_title, $page_redirectTitle, $page_isRedirect, $revision_id, $revision_parentid,
--$revision_timestamp, $revision_userid, $revision_username, $revision_length, $revision_links_string

-- NB: misnomer: it's really the link history, not the deltas (Ashwin mixed up the names).
edits = LOAD '/user/ashwinp/revision_dump_deltas_20150403/*' USING PigStorage('\t')
--edits = LOAD '/tmp/pig.txt' USING PigStorage('\t')
	AS (id:int, page_title:chararray, x1:chararray, x2:chararray, x3:chararray, x4:chararray,
		date:chararray, x5:chararray, x6:chararray, x7:chararray, links:chararray);

edits = FOREACH edits GENERATE
	REPLACE(page_title, ' ', '_') AS page_title,
	SecondsBetween(ToDate(date, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC'), ToDate('2015-01-01T00:00:00Z', 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC')) AS jan_diff,
	SecondsBetween(ToDate(date, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC'), ToDate('2015-04-01T00:00:00Z', 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'', 'UTC')) AS mar_diff,
	date,
	REPLACE(links, ' ', '_') AS links;

edits_jan = FILTER edits BY jan_diff < 0;

------------------ BUG!!!!!!!!!!!!
groups_jan = GROUP edits_jan BY page_title;
top_jan = FOREACH groups_jan {
	sorted = ORDER edits_jan BY jan_diff DESC;
	top = LIMIT sorted 1;
	GENERATE FLATTEN(top);
};

--STORE top_jan INTO '/user/west1/_top_jan';

edits_mar = FILTER edits BY mar_diff > 0;
groups_mar = GROUP edits_mar BY page_title;
top_mar = FOREACH groups_mar {
	sorted = ORDER edits_mar BY mar_diff ASC;
	top = LIMIT sorted 1;
	GENERATE FLATTEN(top);
};

--STORE top_mar INTO '/user/west1/_top_mar';

top = JOIN top_jan BY page_title, top_mar BY page_title;

top = FOREACH top GENERATE
	top_jan::top::page_title,
	top_jan::top::date,
	top_mar::top::date,
	top_jan::top::links,
	top_mar::top::links;

--STORE top INTO '/user/west1/early_and_late_versions_of_all_articles';
