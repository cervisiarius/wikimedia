SET mapreduce.output.fileoutputformat.compress false;

-- Read the Wikidata file that maps Wikidata entries to Wikipedia articles.
Wikidata = LOAD '/user/west1/pagecounts/raw' USING PigStorage('\t')
	AS (mid:chararray, lang:chararray, page_title:chararray, count:long);

Wikidata = FILTER Wikidata BY (INDEXOF(mid, 'Q', 0) == 0);

STORE Wikidata INTO '/user/west1/pagecounts/wikidata_only';
