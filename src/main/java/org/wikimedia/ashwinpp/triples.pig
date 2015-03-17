REGISTER TraceAnalyzer.jar;
A = load '/home/ashwinpp/tree_sample_en.txt' AS (str: chararray);
B = limit A 10;
C = foreach B generate TraceAnalyzer.triplesFromTrees(str);
DUMP C
--STORE C INTO 'output';
