import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from operator import add
import csv
import StringIO
import matplotlib.pyplot as plt 
import argparse
import os
import codecs
import pandas as pd


"""
spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 12 \
--executor-memory 10g \
--executor-cores 8 \
--driver-memory 10g \
./overlap.py -s en -t es -k 2 -r 0.01 -n 10
"""

"""
For each K = 1, ..., K_max
  For each ES editor S:
    For each article X missing in ES but existing in EN:
      N := number of EN editors who have edited X and have overlap at
least K with S
    M := number of X's for which N is at least 1
  Compute the histogram of M across all S
"""

def get_parser(names):
    def loadRecord(line):
        #input = StringIO.StringIO(line)
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord

def compute_overlap(c):
    s_editor, s_editor_items = c[0]
    t_editor, t_editor_items = c[1]
    overlap = len(s_editor_items.intersection(t_editor_items))
    return (t_editor, overlap)

def compute_M_histogram(k, missing_list, source_editors, target_editors, max_num_missing):
    N = []
    for i in range(min(max_num_missing,len(missing_list))):
        print i
        missing_article = missing_list[i]
        reduced_source_editors = source_editors.filter(lambda p: missing_article in p[1])
        source_cartesian_target = reduced_source_editors.cartesian(target_editors)
        missing_article_overlaps = source_cartesian_target.map(compute_overlap).filter(lambda x: x[1] >= k)
        # distinct is too slow, so lets try grouping by user id and taking the keys
        N_x = missing_article_overlaps.map(lambda x: (x[0], missing_article)).groupByKey().keys().collect()
        print N_x
        N += N_x

    return N

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--s', required = True, help='source language' )
    parser.add_argument('-t', '--t', required = True, help='target language' )
    parser.add_argument('-r', '--r', type = float, default = 0.01, help='target language editor sampling rate' )
    parser.add_argument('-k', '--k',  required = True, help = 'min overlap')
    parser.add_argument('-n', '--n',  required = True, type = int, help = 'min overlap')
    args = parser.parse_args()

    s = args.s
    t = args.t
    k = args.k
    r = args.r
    n = args.n

    print args


    # set up environment
    conf = SparkConf()
    conf.set("spark.app.name", 'overlap')
    sc = SparkContext(conf=conf)

    missing_articles_file_name = 'important_' + s +'_articles_missing_in_'+ t +'.tsv'

    if os.path.isfile(missing_articles_file_name):
        missing_list = list(pd.read_csv(missing_articles_file_name, sep = '\t')['id'])
        missing_set = set(missing_list)
    else:
        names = ["importance", "id", "category", "description"]  
        top_articles = sc.textFile("top_10k_wikidata_entities.tsv").map(get_parser(names))
        top_article_ids = top_articles.map(lambda x: x['id'])

        names = ["id", "language_code", "article_name"]
        interlanguage_links = sc.textFile("/user/west1/interlanguage_links.tsv").map(get_parser(names))
        
        # get set of articles that exist in english but not in Spanish
        items_in_source = interlanguage_links.filter(lambda x: x['language_code'] == s).map(lambda x: x['id'])
        items_in_target = interlanguage_links.filter(lambda x: x['language_code'] == t).map(lambda x: x['id'])
        source_items_missing_in_target = items_in_source.subtract(items_in_target)
        missing_list = source_items_missing_in_target.intersection(top_article_ids).collect()
        missing_set = set(missing_list)
        missing_set_details = interlanguage_links.filter(lambda x: x['language_code'] == s and x['id'] in missing_set ).collect()
        with open(missing_articles_file_name, 'w') as f:
            f.write('id\tname\n')
            for d in missing_set_details:
                line = d['id'] + '\t' + d['article_name'] + '\n'
                f.write(line.encode('utf8'))
    

    names = ["language_code", "user_id", "user", "id","page_title","num_edits","bytes_added"]
    source_revision_histories = sc.textFile("/user/west1/revision_history_aggregated/" + s).map(get_parser(names))
    target_revision_histories = sc.textFile("/user/west1/revision_history_aggregated/" + t).map(get_parser(names))

    # sets of articles edited by each editor
    source_editor_to_items = source_revision_histories.map(lambda x: (x['user_id'], x['id'])).groupByKey()
    target_editor_to_items = target_revision_histories.map(lambda x: (x['user_id'], x['id'])).groupByKey()
    # Only consider editors with n or more edits
    n_min = 5
    n_max = 1000

    source_editor_to_items = source_editor_to_items.filter(lambda p: len(p[1]) > n_min and len(p[1]) < n_max)
    target_editor_to_items = target_editor_to_items.filter(lambda p: len(p[1]) > n_min and len(p[1]) < n_max)

    # make items a set instead of a list
    source_editor_to_items = source_editor_to_items.map(lambda x: (x[0], set(x[1])))
    target_editor_to_items = target_editor_to_items.map(lambda x: (x[0], set(x[1])))

    # find source editors who edited important articles missing in target
    def edited_important(pair):
        edited_items = set(pair[1])
        return len(missing_set.intersection(edited_items)) > 0

    reduced_source_editor_to_items = source_editor_to_items.filter(edited_important)
    reduced_source_editor_to_items.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK )

    #persist commonly used RDDs

    target_editor_to_items = target_editor_to_items.sample(False, r, 2)
    target_editor_to_items.persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)
    num_source_editors =  reduced_source_editor_to_items.count()
    num_target_editors = target_editor_to_items.count()

    print num_source_editors, num_target_editors

    N = compute_M_histogram(k, missing_list, reduced_source_editor_to_items, target_editor_to_items, n)
    M = pd.Series(N).value_counts().value_counts()
    M.loc[0] = num_target_editors - len(pd.Series(N).value_counts())
    # add editors with 0 overlap
    M = M.sort_index()
    M.to_csv('_'.join([s, t, str(r), str(k)]) + '.tsv')


    sc.stop()

