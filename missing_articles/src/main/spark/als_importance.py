"""
spark-submit   \
--driver-memory 5g --master yarn --deploy-mode client \
--num-executors 2 \
--executor-memory 10g \
--executor-cores 8 \
--queue priority \
/home/ellery/wikimedia/missing_articles/src/main/spark/als_importance.py \
--config /home/ellery/wikimedia/missing_articles/missing_articles.ini 

"""


from pyspark.sql import *
from operator import add
import csv
from pyspark.mllib.recommendation import ALS, Rating
from random import randint
import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark.mllib.recommendation import ALS
import random 
import numpy
import itertools
from pprint import pprint
from pyspark import SparkConf, SparkContext
import math
import argparse
from ConfigParser import SafeConfigParser
import os


def get_parser(names):
    def loadRecord(line):
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord

def get_lang2id_maps(languages):
    lang2id = {}
    id2lang = {}
    for i, lang in enumerate(languages):
        lang2id[lang] = i
        id2lang[i] = lang
    return lang2id, id2lang


#languages = ['ar', 'bg', 'ca', 'ceb', 'cs','da','de','el','en','eo','es','et','eu','fa','fi','fr','gl','he','hi','hr','hu','hy','id','it','ja','kk','ko','lt','ms','nl','nn','no','pl','pt','ro','ru','sh','simple','sk','sl','sr','sv','tr','uk','uz','vi','war','zh','min','vo']
languages = ['en', 'de', 'fr', 'es', 'nl', 'ru', 'ja']
languages_set = set(languages)
lang2id, id2lang = get_lang2id_maps(languages)



def toRating(x):
    user_id = lang2id[x['lang']]
    item_id=int(x['id'][1:])
    counts = float(x['pageview_count'])
    return (user_id, item_id, counts)


def load_pageviews(sc, filename, k=3):
    names = [ 'id', 'lang', 'title', 'pageview_count']

    pageviews = sc.textFile(filename)\
    .map(get_parser(names))\
    .filter(lambda x: len(x) == 4)\
    .filter(lambda x: x['lang'] in languages_set)\
    .map(lambda x: (x['id'], x))\
    .groupByKey().filter(lambda x: len(x[1]) >=k ).flatMap(lambda x: x[1])\
    .map(toRating)

    user_normalization_constants = dict(pageviews.map(lambda x: (x[0], x[2])).reduceByKey(add).collect())
    print "NORMALIZATION CONSTANTS"
    pprint(user_normalization_constants)

    pageviews = pageviews.map(lambda x: (x[0], x[1], x[2]/user_normalization_constants[x[0]]))

    #pprint(pageviews.take(100))

    return pageviews


def print_triplet_stats(triples):
    numTriples = triples.count()
    numEditors = triples.map(lambda r: r[0]).distinct().count()
    numItems = triples.map(lambda r: r[1]).distinct().count()
    print "Got %d ratings from %d users on %d items." % (numTriples, numEditors, numItems)



def split_train_validation_test(triples, splits = [6, 8], numPartitions = 1000):
    # split ratings into train (60%), validation (20%), and test (20%) 
    random.seed(2)
    split_triples = triples.map(lambda x: (random.randint(0,9),x ))
    split1, split2 = splits
    
    training = split_triples.filter(lambda x: x[0] < split1) \
      .values() \
      .repartition(numPartitions) \
      .cache()
    validation = split_triples.filter(lambda x: x[0] >= split1 and x[0] < split2) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    test = split_triples.filter(lambda x: x[0] >= split2).values().cache()
    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()
    print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
    return training, validation, test



def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))



def cross_validation(training, validation, test, all_triplets, ranks, lambdas, numIters):
# train models and evaluate them on the validation set

    result_dict = {}
    result_template = "rank:%d  iters:%d  lambda: %f"
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1
    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()
    
    for lmbda, numIter, rank in itertools.product(lambdas, numIters, ranks):

        model = ALS.train(training, rank, numIter, nonnegative=True)
        validationRmse = computeRmse(model, validation, numValidation)
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.4f, and numIter = %d." % (rank, lmbda, numIter)


        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter
        result_dict[result_template % (rank, numIter, lmbda)] = validationRmse
    testRmse = computeRmse(bestModel, test, numTest)
    # evaluate the best model on the test set
    print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
      + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)
    result_dict['BEST Model on Test:' + result_template % (bestRank, bestNumIter, bestLambda)] = testRmse
    # compare the best model with a naive baseline that always returns the mean rating
    meanRating = training.union(validation).map(lambda x: x[2]).mean()
    baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
    improvement = (baselineRmse - testRmse) / baselineRmse * 100
    print "The best model improves the baseline by %.2f" % (improvement) + "%."
    result_dict['BEST gain over baseline'] = improvement

    return bestModel, result_dict

def main(args):

    conf = SparkConf()
    conf.set("spark.app.name", 'wikidata item importance estimation')
    sc = SparkContext(conf=conf)

    cp = SafeConfigParser()
    cp.read(args.config)
    pageviews = load_pageviews(sc, cp.get('general', 'pageviews')) 
    training, validation, test = split_train_validation_test(pageviews)

    # cv parameters
    ranks = [2, 5]
    lambdas = [1.0, 100.0 ]
    numIters = [ 5, 15  ]

    model, cv_results = cross_validation(training, validation, test, pageviews, ranks, lambdas, numIters)
    pprint(cv_results)



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required = True, help='full path to configuration file')
    args = parser.parse_args()
    main(args)


    
    


