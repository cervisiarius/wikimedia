hostname                string                  from deserializer   
sequence                bigint                  from deserializer   
dt                      string                  from deserializer   
time_firstbyte          double                  from deserializer   
ip                      string                  from deserializer   
cache_status            string                  from deserializer   
http_status             string                  from deserializer   
response_size           bigint                  from deserializer   
http_method             string                  from deserializer   
uri_host                string                  from deserializer   
uri_path                string                  from deserializer   
uri_query               string                  from deserializer   
content_type            string                  from deserializer   
referer                 string                  from deserializer   
x_forwarded_for         string                  from deserializer   
user_agent              string                  from deserializer   
accept_language         string                  from deserializer   
x_analytics             string                  from deserializer   
range                   string                  from deserializer   
webrequest_source       string                  Source cluster      
year                    int                     Unpadded year of request
month                   int                     Unpadded month of request
day                     int                     Unpadded day of request
hour                    int                     Unpadded hour of request


SELECT
    ip
  , dt
  , SUBSTR(uri_path, 7) AS uri_path
  , uri_query       -- needed?
  , referer
  , user_agent
  --, uri_host --for later, when we consider more projects
  --, content_type -- do we care about this?
  --, referer -- we may need this for figuring out https requests
  --, x_forwarded_for -- same as referer
FROM
  webrequest
WHERE
  http_status = 200 -- should we consider 400 as well? as an attempt to "search" for a page that can't be found?
  AND YEAR = 2014
  AND MONTH = 12
  AND DAY = 1
  AND HOUR = 12
  AND uri_host = "pt.wikipedia.org"
  AND uri_path LIKE "/wiki/%"
ORDER BY
  ip, user_agent, dt
LIMIT 50;



hive  -e "USE wmf_raw; select * from webrequest TABLESAMPLE(BUCKET 1 OUT OF 1000 ON rand()) WHERE http_status = 200 AND uri_host LIKE '%.wikipedia.org' AND YEAR = 2014 AND MONTH = 12 AND DAY = 1 AND HOUR = 12 LIMIT 10000;" > ./sample.tsv


TODO:

keep searches

filter mobile

filter apps
content_type: application/json

filter language

traces



x_forwarded_for: sometimes several in same entry -> take last

