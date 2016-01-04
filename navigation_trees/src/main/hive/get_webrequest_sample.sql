SELECT
  dt
  , user_agent
  , uri_path
  , http_status
  , referer
  , accept_language
FROM
  webrequest
WHERE
  year = 2016
  AND month = 1
  AND day = 1
  AND hour = 1
  AND uri_host = "en.wikipedia.org"
LIMIT 1000;


--  , geocoded_data['country_code'] AS country_code,
--  , geocoded_data['city'] AS city
