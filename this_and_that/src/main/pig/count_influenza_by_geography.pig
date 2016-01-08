/*
pig \
-param PARALLEL=200 \
count_influenza_by_geography.pig
*/

SET mapreduce.output.fileoutputformat.compress false;

Trees = LOAD '/user/west1/navigation_trees/year=2015/month=12/en/' USING PigStorage('\t')
    AS (json:chararray);

Data = FOREACH Trees GENERATE
    (json MATCHES '.*"title":"Influenza".*' ? 1 : 0) AS contains_influenza,
    REGEX_EXTRACT(json, '.*"country":"(.*?)".*', 1) AS country,
    REGEX_EXTRACT(json, '.*"state":"(.*?)".*', 1) AS state;

-- Aggregate by country and state.
Grouped = GROUP Data BY (country, state) PARALLEL $PARALLEL;
Counts = FOREACH Grouped GENERATE
    group.country AS country,
    group.state AS state,
    COUNT(Data) AS all_trees,
    SUM(Data.contains_influenza) AS flu_trees;

-- -- Aggregate by country.
-- ByCountry = GROUP Data BY country PARALLEL $PARALLEL;
-- CountryCounts = FOREACH ByCountry GENERATE
--     group AS country,
--     COUNT(ByCountry) AS all_trees,
--     SUM(ByCountry.contains_influenza) AS flu_trees;

-- -- Aggregate by state for subset of US trees.
-- USData = FILTER Data BY (country == 'US');
-- ByState = GROUP USData BY state PARALLEL $PARALLEL;
-- StateCounts = FOREACH ByState GENERATE
--     group AS state,
--     COUNT(ByState) AS all_trees,
--     SUM(ByState.contains_influenza) AS flu_trees;

STORE Counts INTO '/user/west1/health/influenza_per_country_and_state';
