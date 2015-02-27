CREATE TABLE IF NOT EXISTS west1.pageview_counts_enwiki (
  article STRING,
  n BIGINT
);

# Output goes to hdfs://analytics-hadoop/user/hive/warehouse/west1.db/pageview_counts_enwiki/

INSERT INTO TABLE west1.pageview_counts_enwiki
  SELECT curr, SUM(n) FROM ellery.clickstream_v0_5 WHERE year=2015 GROUP BY curr;
