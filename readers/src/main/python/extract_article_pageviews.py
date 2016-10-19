import re
from pyspark import SparkContext, SparkConf

"""
Usage:

spark-submit \
--driver-memory 10g \
--master yarn \
--deploy-mode client \
--num-executors 80 \
--executor-memory 6g \
--executor-cores 4 \
extract_article_pageviews.py

spark-submit \
--driver-memory 2g \
--master local \
--deploy-mode client \
--num-executors 1 \
--executor-memory 2g \
--executor-cores 1 \
extract_article_pageviews.py
"""

BASE_DIR = "/user/west1/"
# BASE_DIR = "file:///tmp/"

# The input data comes from here: https://dumps.wikimedia.org/other/pagecounts-ez/merged/
# Example row:
# aa.b Special:SpecialPages 17 BB1F1X2,DW1,EB1,KE1,OR1,PM1X1,RC1,SM1,VN1,[R1,\X1,^G1,_L1,
infile = BASE_DIR + "pagecounts-2016-03-views-ge-5.txt"
outfile = BASE_DIR + "pagecounts-2016-03_FIRST-WEEK-OF-MARCH"

# Code for English Wikipedia.
project_code = "en.z"

# Exclude pages such as "Talk:...", "User:...", etc.
PAGE_EXCLUDE_REGEX = re.compile(r'.*:[^ _].*')

sc = SparkContext(conf=SparkConf().setAppName('extract_article_pageviews.py') \
                                  .set('spark.rdd.compress', 'true'))

def day_char_to_int(char):
  return ord(char) - 64

def get_weekly_count(count_string):
  if count_string.endswith(","):
    count_string = count_string[:len(count_string)-1]
  days = count_string.split(",")
  days = filter(lambda s: day_char_to_int(s[0]) <= 8, days)
  daily_counts = map(lambda s: sum(int(x) for x in re.split(r"[A-X]", s[2:])), days)
  return sum(daily_counts)

data = sc.textFile(infile) \
         .filter(lambda line: not line.startswith("#")) \
         .map(lambda line: tuple(line.strip().split(" "))) \
         .filter(lambda (project, page, monthly_count, hourly_count_string): project == project_code and not PAGE_EXCLUDE_REGEX.match(page)) \
         .map(lambda (project, page, monthly_count, hourly_count_string): (page.replace(' ', '_'), monthly_count, str(get_weekly_count(hourly_count_string)))) \
         .map(lambda x: '\t'.join(x))

data.saveAsTextFile(outfile)
