/*
pig \
-param PARALLEL=200 \
count_smoking_by_geography.pig
*/

%declare LANG 'de'
%declare TOBACCO 'Tabak'
%declare LUNGCANCER 'Bronchialkarzinom'
%declare ECIGARETTE 'Elektrische_Zigarette'
%declare NICOTINE 'Nicotin'
%declare CESSATION 'Tabaksucht'

-- %declare LANG en
-- %declare TOBACCO Tobacco
-- %declare LUNGCANCER Lung_cancer
-- %declare ECIGARETTE Electronic_cigarette
-- %declare NICOTINE Nicotine
-- %declare CESSATION Smoking_cessation

-- %declare LANG ru
-- %declare TOBACCO Табак_\\\\((сырьё|вещество)\\\\)
-- %declare LUNGCANCER Рак_лёгкого
-- %declare ECIGARETTE Электронная_сигарета
-- %declare NICOTINE Никотин
-- %declare CESSATION Лечение_никотиновой_зависимости

-- %declare LANG sv
-- %declare TOBACCO Tobak
-- %declare LUNGCANCER Lungcancer
-- %declare ECIGARETTE Elektronisk_cigarett
-- %declare NICOTINE Nikotin
-- %declare CESSATION Rökavvänjning

SET mapreduce.output.fileoutputformat.compress false;

Trees = LOAD '/user/west1/navigation_trees/year=2015/month=1[12]/$LANG/' USING PigStorage('\t')
    AS (json:chararray);

Data = FOREACH Trees GENERATE
    (json MATCHES '.*\\"title\\":\\"$TOBACCO\\".*' ? 1 : 0) AS contains_tobacco,
    (json MATCHES '.*\\"title\\":\\"$LUNGCANCER\\".*' ? 1 : 0) AS contains_lungcancer,
    (json MATCHES '.*\\"title\\":\\"$ECIGARETTE\\".*' ? 1 : 0) AS contains_ecigarette,
    (json MATCHES '.*\\"title\\":\\"$NICOTINE\\".*' ? 1 : 0) AS contains_nicotine,
    (json MATCHES '.*\\"title\\":\\"$CESSATION\\".*' ? 1 : 0) AS contains_cessation,
    REGEX_EXTRACT(json, '.*\\"country\\":\\"(.*?)\\".*', 1) AS country,
    REGEX_EXTRACT(json, '.*\\"state\\":\\"(.*?)\\".*', 1) AS state;

-- Aggregate by country and state.
Grouped = GROUP Data BY (country, state) PARALLEL $PARALLEL;
Counts = FOREACH Grouped GENERATE
    group.country AS country,
    group.state AS state,
    COUNT(Data) AS all_trees,
    SUM(Data.contains_tobacco),
    SUM(Data.contains_lungcancer),
    SUM(Data.contains_ecigarette),
    SUM(Data.contains_nicotine),
    SUM(Data.contains_cessation);

STORE Counts INTO '/user/west1/health/smoking_per_country_and_state/$LANG';
