from pyspark import SparkConf, SparkContext
from ConfigParser import SafeConfigParser
from util import save_rdd, get_parser
from ConfigParser import SafeConfigParser
import os
import json
import argparse


"""
/home/otto/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
--driver-memory 5g --master yarn --deploy-mode client \
--num-executors 2 --executor-memory 10g --executor-cores 8 \
--queue priority \
/home/ellery/wikimedia/missing_articles/src/main/spark/aggregate_contributions.py \
--dir en-es \
--lang es \
--config /home/ellery/wikimedia/missing_articles/missing_articles.ini 
"""


def to_str(t):
    uid, contributions = t
    contributions = list(contributions)
    uname = contributions[0]['user']
    contributions = [{k: d[k] for k in ('id', 'page_title', 'num_edits', 'timestamp', 'bytes_added')} for d in contributions]
    contributions.sort(key = lambda x: x['timestamp'])
    obj = {'uid': uid, 'uname': uname, 'contributions': contributions}
    return json.dumps(obj)
        

def main(args, sc):
	exp_dir = args.dir
	language = args.lang
	cp = SafeConfigParser()
	cp.read(args.config)
	base_dir = os.path.join(cp.get('general', 'local_data_dir'), exp_dir)
	hadoop_base_dir = os.path.join(cp.get('general', 'hadoop_data_dir'), exp_dir)

	names = ["language_code", "user_id", "user", "id","page_title","num_edits","timestamp", "bytes_added"]
	contributions_file = os.path.join(cp.get('general', 'contributions_dir'), language)
	contributions = sc.textFile(contributions_file).map(get_parser(names)).filter(lambda x: len(x) == 8)
	contributions = contributions.map(lambda x: (x['user_id'], x)).groupByKey()
	contributions = contributions.map(to_str)
	save_rdd(contributions, base_dir , hadoop_base_dir, cp.get('eval', 'contributions'))



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required = True, help='experiment directory')
    parser.add_argument('--config', required = True, help='full path to configuration file')
    parser.add_argument('--lang', required = True, help='language of the articles in the corpus')
    args = parser.parse_args()

    conf = SparkConf()
    conf.set("spark.app.name", 'aggregate contributions')
    sc = SparkContext(conf=conf)

    main(args, sc)

