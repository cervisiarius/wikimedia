A = load '/home/ashwinpp/tree_sample_en.txt' AS (str: chararray);
B = limit A 10;
DUMP B;
