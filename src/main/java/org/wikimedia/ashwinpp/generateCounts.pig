rmf output.singletons
rmf output.pairs
rmf output.triples
REGISTER TraceAnalyzer.jar;
%default threshold  20;
trees = load 'hdfs:///user/west1/navigation_trees' AS (str:chararray);
--trees = load '/home/ashwinpp/tree_sample_en.txt' AS (str: chararray);
trees = limit trees 1000000;

singletons = foreach trees generate FLATTEN(TraceAnalyzer.singletonsFromTrees(str));
singletonGroups = group singletons by (*);
singletonCounts = foreach singletonGroups generate group, COUNT(singletons);
--singletonCounts = filter singletonCounts by $1>$threshold;
STORE singletonCounts INTO 'output.singletons';

pairs = foreach trees generate FLATTEN(TraceAnalyzer.pairsFromTrees(str));
pairsGroups = group pairs by (*);
pairsCounts = foreach pairsGroups generate group, COUNT(pairs);
--pairsCounts = filter pairsCounts by $1>$threshold;
STORE pairsCounts INTO 'output.pairs';

--pab = join pairsCounts by $0.$0, singletonCounts by $0;
--pab = foreach pab generate $0, ((float)$1)/$3;
--STORE pab INTO 'output.pab';

triples = foreach trees generate FLATTEN(TraceAnalyzer.triplesFromTrees(str));
triplesGroups = group triples by (*);
triplesCounts = foreach triplesGroups generate group, COUNT(triples);
STORE triplesCounts INTO 'output.triples';




