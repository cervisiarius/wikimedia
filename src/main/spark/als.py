from pyspark.sql import *
from operator import add
import csv
import StringIO
import matplotlib.pyplot as plt 
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

# remove "ratings" if the editor added less that MIN_BYTES to the article
MIN_BYTES = 100
# remove the editor is they have edited more than MAX_ARTICLES or less than MIN_ARTICLES
MIN_ARTICLES = 5
MAX_ARTICLES = 1000

#sampling
PRE_REDUCTION_RATE = 1.0
POST_REDUCTION_RATE = 0.01

# what language to buld recommender for 
LANGUAGE = 'en'

# cv parameters
ranks = [25, 35  ]
lambdas = [1.0, 10.0  ]
numIters = [10,20 ]

conf = SparkConf()
conf.set("spark.app.name", 'als')
sc = SparkContext(conf=conf)



def get_parser(names):
    def loadRecord(line):
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord


def load_contributions_rdd(language):
    """
    Takes in a language, returns an RDD of the projects aggregated revison history
    """
    language = 'es' 
    names = ["language_code", "user_id", "user", "id","page_title","num_edits","bytes_added"]
    revision_histories = sc.textFile("/user/west1/revision_history_aggregated/" + language).map(get_parser(names))
    # For some reason, a few rows have less than 7 cells. remove them
    revision_histories = revision_histories.filter(lambda x: len(x) == len(names))
    contributions = revision_histories.map(lambda x: (int(x['user_id']), int(x['id'][1:]), float(x['bytes_added'])))
    return contributions


def reduce_contributions(contributions, min_bytes, min_articles, max_articles, pre_reduction_rate, post_reduction_rate):
    """
    To reduce computational complexity, we can try to reduce the number of contributions by:
    1. removing contributons of less then min_bytes bytes_added
    2. removing editirs with too many or too few contributions
    """
    if pre_reduction_rate < 1.0:
        contributions = contributions.sample(False, pre_reduction_rate, 2)
    reduced_contributions = contributions.filter(lambda x: x[2] > min_bytes)
    contributions_by_user = reduced_contributions.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().filter(lambda x: len(x[1]) >=min_articles and len(x[1]) <= max_articles)
    reduced_contributions = contributions_by_user.flatMap(lambda x: [(x[0], l[0], l[1]) for l in x[1]])
    if post_reduction_rate < 1.0:
        return reduced_contributions.sample(False, post_reduction_rate, 2)
    else:
        return reduced_contributions


def print_contribution_stats(contributions):
    numContributions = contributions.count()
    numEditors = contributions.map(lambda r: r[0]).distinct().count()
    numItems = contributions.map(lambda r: r[1]).distinct().count()
    print "Got %d ratings from %d users on %d items." % (numContributions, numEditors, numItems)



def split_train_validation_test(contributions, splits = [6, 8], numPartitions = 100):
    # split ratings into train (60%), validation (20%), and test (20%) 
    random.seed(2)
    split_contributions = contributions.map(lambda x: (random.randint(0,9),x ))
    split1, split2 = splits
    # get evlauation editor item sets and flatten dict of lists into a list
    eval_dict = get_qualitative_evaluation_editors()
    eval_contributions =   list(itertools.chain.from_iterable(eval_dict.values())) 
    training = split_contributions.filter(lambda x: x[0] < split1) \
      .values() \
      .union(sc.parallelize(eval_contributions)) \
      .repartition(numPartitions) \
      .cache()
    validation = split_contributions.filter(lambda x: x[0] >= split1 and x[0] < split2) \
      .values() \
      .repartition(numPartitions) \
      .cache()
    test = split_contributions.filter(lambda x: x[0] >= split2).values().cache()
    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()
    print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
    return training, validation, test


def get_qualitative_evaluation_editors():
    # Item Sets
    vw_junky = (246, 622618, 697152, 835078, 247, 2506880)
    bio_nerd = (7187, 178694, 7430, 8066, 37748, 19088, 39572, 47263)
    hesse_fan = (576493, 2171354,860577, 457289, 217073)
    werner_herzog_fan = (1113073, 688329,325662,695888)
    ratings = {}
    ratings['vw_junky'] = [(9999991, i, random.randint(100, 1000)) for i in vw_junky]
    ratings['bio_nerd'] = [(9999992, i, random.randint(100, 1000)) for i in bio_nerd]
    ratings['hesse_fan'] = [(9999993, i, random.randint(100, 1000)) for i in hesse_fan]
    ratings['werner_herzog'] = [(9999994, i, random.randint(100, 1000)) for i in werner_herzog_fan]
    return ratings


def qualitative_evaluation(model, training):
    result_dict = {}
    candidates = training.map(lambda x: x[1]).distinct().persist()
    names = ["id", "x", 'x', 'name', 'x', 'x', 'x']
    id_title_map = sc.textFile("/user/ellery/missing_and_exisiting_for_top_50_langs.tsv.gz").map(get_parser(names))
    id_title_map = id_title_map.map(lambda x: (int(x['id'][1:]), x['name']))
    id_title_map = id_title_map.repartition(1000).persist()
    print id_title_map.take(100)
    for editor, ratings in get_qualitative_evaluation_editors().iteritems():
        result_dict[editor] = []
        editor_id = ratings[0][0]
        predictions = model.predictAll(candidates.map(lambda x: (editor_id, x)))
        predictions = predictions.top(25, key=lambda x: x[2])
        print "Articles recommended for %s:" % editor
        for i in xrange(len(predictions)):
            item_id = predictions[i][1]
            item_name = id_title_map.lookup(item_id)
            print ("%2d: %d, %s" % (i + 1, item_id, item_name)).encode('ascii', 'ignore')
            result_dict[editor].append((i, 'Q' +str(item_id), item_name, predictions[i][2]))
    return result_dict



def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))





def cross_validation(training, validation, test, ranks, lambdas, numIters):
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
    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lmbda)
        validationRmse = computeRmse(model, validation, numValidation)
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
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

if __name__ == '__main__':
    contributions = reduce_contributions(load_contributions_rdd(LANGUAGE), MIN_BYTES, MIN_ARTICLES, MAX_ARTICLES, PRE_REDUCTION_RATE, POST_REDUCTION_RATE)
    training, validation, test = split_train_validation_test(contributions)
    model, cv_results = cross_validation(training, validation, test, ranks, lambdas, numIters)
    pprint(cv_results)
    qe_results = qualitative_evaluation(model, training)
    sc.stop()
    pprint(cv_results)
    pprint(qe_results)

    


