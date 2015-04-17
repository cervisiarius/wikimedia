SET mapreduce.output.fileoutputformat.compress false;
--rmf output.singletons
--rmf output.pairs
--rmf output.triples
REGISTER TraceAnalyzer.jar;
%default threshold  100;
trees = load 'hdfs:///user/west1/navigation_trees/en/*' AS (str:chararray);
--trees = load '/home/ashwinpp/tree_sample_en.txt' AS (str: chararray);

singletons = foreach trees generate FLATTEN(TraceAnalyzer.singletonsFromTrees(str));
singletonsAugmented = foreach singletons generate (*) as page, ROUND(RANDOM()*100) as rnd;
singletonGroups = group singletonsAugmented by (*) parallel 100;
singletonCounts = foreach singletonGroups generate group, COUNT(singletonsAugmented) as count;
singletonGroups = group singletonCounts by group.page; 
singletonCounts = foreach singletonGroups generate FLATTEN(group), SUM(singletonCounts.count) as count;

singletonCounts = filter singletonCounts by count>$threshold;
STORE singletonCounts INTO 'output.singletons';

pairs = foreach trees generate FLATTEN(TraceAnalyzer.pairsFromTrees(str));
pairsAugmented = foreach pairs generate (*) as page, ROUND(RANDOM()*100) as rnd;
pairsGroups = group pairsAugmented by (*) parallel 100;
pairsCounts = foreach pairsGroups generate group, COUNT(pairsAugmented) as count;
pairsGroups = group pairsCounts by group.page; 
pairsCounts = foreach pairsGroups generate FLATTEN(group), SUM(pairsCounts.count) as count;
pairsCounts = filter pairsCounts by count>$threshold;
STORE pairsCounts INTO 'output.pairs';

triples = foreach trees generate FLATTEN(TraceAnalyzer.triplesFromTrees(str));
triplesAugmented = foreach triples generate (*) as page, ROUND(RANDOM()*100) as rnd;
triplesGroups = group triplesAugmented by (*) parallel 100;
triplesCounts = foreach triplesGroups generate group, COUNT(triplesAugmented) as count;
triplesGroups = group triplesCounts by group.page; 
triplesCounts = foreach triplesGroups generate FLATTEN(group), SUM(triplesCounts.count) as count;
triplesCounts = filter triplesCounts by count>$threshold;
STORE triplesCounts INTO 'output.triples';




