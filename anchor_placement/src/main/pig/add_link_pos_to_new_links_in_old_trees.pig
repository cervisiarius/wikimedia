/*
pig \
-param PARALLEL=10 \
add_link_pos_to_new_links_in_old_trees.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

-- Load the new links in old trees.
--Triples = LOAD '/user/west1/anchor_placement/new_links_in_old_trees' USING PigStorage('\t')
Triples = LOAD '/afs/cs.stanford.edu/u/west1/anchor_placement/new_links_in_old_trees' USING PigStorage('\t')
  AS (tree_id:chararray, length_st:int, s:chararray, m:chararray, t:chararray);

-- Load link positions.
--Pos = LOAD '/user/west1/wiki_parsed/link_positions_20150331000000' USING PigStorage('\t')
Pos = LOAD '/afs/cs.stanford.edu/u/west1/wiki_parsed/link_positions_TEST.tsv' USING PigStorage('\t')
  AS (s:chararray, t:chararray, length:int, pos_list:chararray);

-- Add positions of m.
WithPosM = JOIN Triples BY (s, m), Pos BY (s, t) PARALLEL $PARALLEL;

STORE WithPosM INTO '/tmp/with_pos';
