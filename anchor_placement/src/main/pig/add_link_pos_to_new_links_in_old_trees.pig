/*
pig \
-param PARALLEL=10 \
add_link_pos_to_new_links_in_old_trees.pig
*/

-- This is to be run on S machines, since the position data resides there.

SET mapreduce.output.fileoutputformat.compress false;

-- Load the new links in old trees.
--Triples = LOAD '/user/west1/anchor_placement/new_links_in_old_trees' USING PigStorage('\t')
Triples = LOAD '/afs/cs.stanford.edu/u/west1/wikimedia/trunk/data/anchor_placement/new_links_in_old_trees.tsv' USING PigStorage('\t')
  AS (tree_id:chararray, length_st:int, s:chararray, m:chararray, t:chararray);

-- Load link positions.
--Pos = LOAD '/user/west1/wiki_parsed/link_positions_20150331000000' USING PigStorage('\t')
Pos = LOAD '/afs/cs.stanford.edu/u/west1/wikimedia/trunk/data/anchor_placement/link_positions_TEST.tsv' USING PigStorage('\t')
  AS (s:chararray, t:chararray, num_char:int, pos_list:chararray);

-- Add positions of m.
WithPosM = JOIN Triples BY (s, m), Pos BY (s, t) PARALLEL $PARALLEL;
WithPosM = FOREACH WithPosM GENERATE
  Triples::tree_id AS tree_id,
  Triples::s AS s,
  Triples::m AS m,
  Triples::t AS t,
  Triples::length_st AS length_st,
  Pos::num_char AS num_char,
  Pos::pos_list AS pos_list_m;

-- Add positions of t.
WithPosMT = JOIN WithPosM BY (s, t), Pos BY (s, t) PARALLEL $PARALLEL;
WithPosMT = FOREACH WithPosMT GENERATE
  WithPosM::tree_id AS tree_id,
  WithPosM::s AS s,
  WithPosM::m AS m,
  WithPosM::t AS t,
  WithPosM::length_st AS length_st,
  WithPosM::pos_list_m AS pos_list_m,
  Pos::pos_list AS pos_list_t;

STORE WithPosM INTO '/tmp/with_pos';
