SET mapreduce.output.fileoutputformat.compress false;
rmf output.pab_table
rmf output.pabc_table

sCounts = load 'hdfs:///user/ashwinpp/output.singletons/*' as (current: chararray, count:int);
pCounts = load 'hdfs:///user/ashwinpp/output.pairs/*' as (parent: chararray, current: chararray, count:int);
--sCounts = load 'output.singletons/*' as (current: chararray, count:int);
--pCounts = load 'output.pairs/*' as (parent: chararray, current: chararray, count:int);

pab_table = join pCounts by parent, sCounts by current;
pab_table = foreach pab_table generate pCounts::parent as parent, pCounts::current as current,pCounts::count as pcount,  sCounts::count as scount, ((float)pCounts::count)/sCounts::count as pab;
STORE pab_table INTO 'output.pab_table';

tCounts = load 'hdfs:///user/ashwinpp/output.triples/*' as (grandparent: chararray, parent:chararray, current: chararray, count:int);
--tCounts = load 'output.triplets/*' as (grandparent: chararray, parent:chararray, current: chararray, count:int);

pabc_table1 = join tCounts by (parent, current), pab_table by (parent , current);
--pabc = foreach pabc generate tCounts::grandparent as grandparent, tCounts::parent as parent, tCounts::current as current, tCounts::count as tcount, pCounts
pabc_table2 = join pabc_table1 by (tCounts::grandparent, tCounts::parent), pCounts by (parent, current);

pabc_table2 = foreach pabc_table2 generate pabc_table1::tCounts::grandparent, pabc_table1::tCounts::parent, pabc_table1::tCounts::current, pabc_table1::tCounts::count, pCounts::count, ((float)pabc_table1::tCounts::count)/pCounts::count, pabc_table1::pab_table::pcount, pabc_table1::pab_table::scount, pabc_table1::pab_table::pab, ((float)pabc_table1::tCounts::count)/pCounts::count - pabc_table1::pab_table::pab;

STORE pabc_table2 INTO 'output.pabc_table';
