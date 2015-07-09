--12      Anarchism       67475   43618   2002-04-03T07:36:30Z    17      Peter Winnberg  11407   PeterKropotkin  Peter Kropotkin

REGISTER pigudf.jar;

edits = LOAD '/user/ashwinp/revision_dump_link_history_20150403/*' USING PigStorage('\t')
	AS (id:int, page_title:chararray, x1:chararray, parent:int, date:chararray, x2:chararray,
		x3:chararray, x4:chararray, x5:chararray, x6:chararray);

--edits = FILTER edits BY (parent == -1);

edits = FOREACH edits GENERATE REPLACE(org.wikimedia.west1.pigudf.URLDECODE(page_title), ' ', '_') AS page_title, date;

groups = GROUP edits BY page_title;

firsts = FOREACH groups GENERATE group AS page_title, MIN(edits.date) AS date;

STORE firsts INTO '/user/west1/article_creation_dates';
