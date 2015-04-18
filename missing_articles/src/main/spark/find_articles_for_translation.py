import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from operator import add
import argparse
import os
import codecs
import pandas as pd



"""
Usage: 

/home/otto/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
--driver-memory 5g --master yarn --deploy-mode client \
--num-executors 2 --executor-memory 10g --executor-cores 8 \
/home/ellery/wikimedia/missing_articles/src/main/spark/find_articles_for_translation.py --dir en-siple --s en -t simple

"""

def get_parser(names):
    def loadRecord(line):
        #input = StringIO.StringIO(line)
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', required = True, help='experiment dir' )
    parser.add_argument('-s', '--s', required = True, help='source language' )
    parser.add_argument('-t', '--t', required = True, help='target language' )
    
    args = parser.parse_args()

    s = args.s
    t = args.t
    expdir = os.path.join('/home/ellery', args.dir)

    # set up environment
    conf = SparkConf()
    conf.set("spark.app.name", 'find articles for translation')
    sc = SparkContext(conf=conf)

    important_missing_articles_file_name = 'important_' + s +'_articles_missing_in_'+ t +'.tsv'
    #missing_articles_file_name = s +'_articles_missing_in_'+ t +'.tsv'
    
    names = ["importance", "id", "category", "description"]  
    top_articles = sc.textFile("top_10k_wikidata_entities.tsv").map(get_parser(names))
    top_article_ids = top_articles.map(lambda x: x['id'])

    names = ["id", "language_code", "article_name"]
    interlanguage_links = sc.textFile("/user/west1/interlanguage_links.tsv").map(get_parser(names))
    
    items_in_source = interlanguage_links.filter(lambda x: x['language_code'] == s).map(lambda x: x['id'])
    items_in_target = interlanguage_links.filter(lambda x: x['language_code'] == t).map(lambda x: x['id'])
    source_items_missing_in_target = items_in_source.subtract(items_in_target)


    missing_list = source_items_missing_in_target.intersection(top_article_ids).collect()
    missing_set = set(missing_list)
    missing_set_details = interlanguage_links.filter(lambda x: x['language_code'] == s and x['id'] in missing_set ).collect()
    with open(os.path.join(expdir, important_missing_articles_file_name), 'w') as f:
        f.write('id\tname\n')
        for d in missing_set_details:
            line = d['id'] + '\t' + d['article_name'] + '\n'
            f.write(line.encode('utf8'))