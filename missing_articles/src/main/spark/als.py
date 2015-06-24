"""
/home/otto/spark-1.3.0-bin-hadoop2.4/bin/spark-submit   \
--master yarn \
--deploy-mode client \
--num-executors 15 \
--executor-memory 10g \
--executor-cores 8 \
--driver-memory 10g \
--queue priority \
/home/ellery/wikimedia/missing_articles/src/main/spark/als.py
"""


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
import math

# remove "ratings" if the editor added less that MIN_BYTES to the article
MIN_BYTES = 100
# remove the editor is they have edited more than MAX_ARTICLES or less than MIN_ARTICLES
MIN_ARTICLES = 2
MAX_ARTICLES = 1000

#sampling
PRE_REDUCTION_RATE = 1.0
POST_REDUCTION_RATE = 1.0

IMPLICIT = False

# what language to buld recommender for 
LANGUAGES = ['en',]

# cv parameters
ranks = [100, 400 ]
lambdas = [0.001, 0.1, 1.0, 10.0,  100.0, 1000.0 ]
numIters = [3, 10 ]
alphas = [0.001, 0.1, 40.0]

def score_function(bytes_added, num_edits):
    return math.log(bytes_added)

def reduction_function(t):
    user_id, row_list = t
    row_list = [r for r in row_list if r.bytes_added > MIN_BYTES]



conf = SparkConf()
conf.set("spark.app.name", 'als')
conf.set("spark.core.connection.ack.wait.timeout", "180")



sc = SparkContext(conf=conf)



def get_parser(names):
    def loadRecord(line):
        cells = line.strip().split('\t')
        return dict(zip(names, cells))
    return loadRecord

def parse_rating(x):
    user_id = int(x['user_id'])
    item_id=int(x['id'][1:])
    bytes_added = float(x['bytes_added'])
    num_edits = int(x['num_edits'])
    r = Row(item_id=item_id, bytes_added=bytes_added, num_edits=num_edits)
    return (user_id, r)



def load_single_language_contributions(language, min_bytes, min_articles, max_articles):
    """
    Takes in a language, returns an RDD of the projects aggregated revison history
    """
    names = ["language_code", "user_id", "user", "id","page_title","num_edits","bytes_added"]
    contributions = sc.textFile("/user/west1/revision_history_aggregated/" + language).map(get_parser(names))
    #print "Got %d raw %s contributions" % (contributions.count(), language)
    # For some reason, a few rows have less than 7 cells. remove them
    contributions = contributions.filter(lambda x: len(x) == len(names) and int(x['bytes_added']) > min_bytes)
    # turn each record into a uid, data_row tuple
    #print "Got %d byte-reduced %s contributions" % (contributions.count(), language)
    contributions = contributions.map(parse_rating)
    # group by user_id
    return contributions.groupByKey().filter(lambda x: (len(x[1]) >= min_articles) and (len(x[1]) <= max_articles))



def load_all_contributions(languages, min_bytes, min_articles, max_articles, score_function):
    contributions_by_user = load_single_language_contributions(languages[0], min_bytes, min_articles, max_articles)
    for language in languages[1:]:
        new_contributions_by_user = load_single_language_contributions(language, min_bytes, min_articles, max_articles)
        contributions_by_user = contributions_by_user.union(new_contributions_by_user)

    """
    example = contributions_by_user.take(100)
    for e in example:
        print e[0]
        for i in e[1]:
            print i
    """

    #reindex users (id, list(row)) -> ((id, list(row)), new_id) -> (new_id, list(row)))
    contributions_by_user = contributions_by_user.zipWithIndex().map(lambda x: (x[1], x[0][1]))
    return contributions_by_user.flatMap(lambda x: [(x[0], r.item_id, score_function(r.bytes_added, r.num_edits)) for r in x[1]])

    

def print_contribution_stats(contributions):
    numContributions = contributions.count()
    numEditors = contributions.map(lambda r: r[0]).distinct().count()
    numItems = contributions.map(lambda r: r[1]).distinct().count()
    print "Got %d ratings from %d users on %d items." % (numContributions, numEditors, numItems)



def split_train_validation_test(contributions, splits = [6, 8], numPartitions = 1000):
    # split ratings into train (60%), validation (20%), and test (20%) 
    random.seed(2)
    split_contributions = contributions.map(lambda x: (random.randint(0,9),x ))
    split1, split2 = splits
    # get evlauation editor item sets and flatten dict of lists into a list
    eval_dict = get_qualitative_evaluation_editors(score_function)
    eval_contributions =   list(itertools.chain.from_iterable(eval_dict.values())) 
    #pprint(eval_contributions)
    training = split_contributions.filter(lambda x: x[0] < split1) \
      .values() \
      .union(sc.parallelize(eval_contributions)) \
      .repartition(numPartitions) \
      .cache()
    validation = split_contributions.filter(lambda x: x[0] >= split1 and x[0] < split2) \
      .values() \
      .repartition(numPartitions) \
      .cache()

    #pprint(training.take(100))
    print "Checking that Qualitative Editors in Training set"
    try:
        print training.filter(lambda x: x[0] == 9999991 ).collect()
    except:
        print "look failed"
    
    test = split_contributions.filter(lambda x: x[0] >= split2).values().cache()
    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()
    print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
    return training, validation, test


def get_qualitative_evaluation_editors(score_function):
    # Item Sets

    def create_score():
        bytes_added = random.randint(200, 10000) 
        num_edits = random.randint(1, 15)
        return score_function (bytes_added, num_edits )

    vw_junky = (246, 622618, 697152, 835078, 247, 2506880)
    bio_nerd = (7187, 178694, 7430, 8066, 37748, 19088, 39572, 47263)
    hesse_fan = (576493, 2171354,860577, 457289, 217073)
    werner_herzog_fan = (1113073, 688329,325662,695888)
    ratings = {}
    ratings['vw_junky'] = [(9999991, i, create_score()) for i in vw_junky]
    ratings['bio_nerd'] = [(9999992, i, create_score()) for i in bio_nerd]
    ratings['hesse_fan'] = [(9999993, i, create_score()) for i in hesse_fan]
    ratings['werner_herzog'] = [(9999994, i, create_score()) for i in werner_herzog_fan]
    return ratings


def qualitative_evaluation(model, candidates, id_title_map):
    result_dict = {}
    for editor, ratings in get_qualitative_evaluation_editors(score_function).iteritems():
        result_dict[editor] = []
        editor_id = ratings[0][0]
        print "Getting Predictions"
        predictions = model.predictAll(candidates.map(lambda x: (editor_id, x)))
        print "Getting top values"
        predictions = predictions.top(20, key=lambda x: x[2])
        print "Articles recommended for %s:" % editor
        for i in xrange(len(predictions)):
            item_id = predictions[i][1]
            item_name = id_title_map[item_id]
            print "%2d: %d, %s, %e" % (i + 1, item_id, item_name.encode('ascii', 'ignore'), predictions[i][2])
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

def compute_recall_at_k(model, data, k=25):
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))



def cross_validation(training, validation, test, candidates, id_title_map, ranks, lambdas, numIters, alphas):
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
    if  not IMPLICIT:
        alphas = [1.0]
    for rank, lmbda, numIter, alpha in itertools.product(ranks, lambdas, numIters, alphas):
        if IMPLICIT:
            model = ALS.trainImplicit(training, rank, iterations=numIter, lambda_=lmbda, alpha=alpha, nonnegative=True)
        else:
            model = ALS.train(training, rank, iterations=numIter, lambda_=lmbda, nonnegative=True)
        validationRmse = 0.0 #computeRmse(model, validation, numValidation)
        print "RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.4f, and numIter = %d and alpha=%f." % (rank, lmbda, numIter, alpha)

        qe_results = qualitative_evaluation(model, candidates, id_title_map)

        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter
        result_dict[result_template % (rank, numIter, lmbda)] = validationRmse
    testRmse = 0.0 #computeRmse(bestModel, test, numTest)
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


    
    contributions = load_all_contributions(LANGUAGES, MIN_BYTES, MIN_ARTICLES, MAX_ARTICLES, score_function) 
    training, validation, test = split_train_validation_test(contributions)
    candidates = training.map(lambda x: x[1]).distinct().persist()
    
    names = ["id", "x", 'x', 'name', 'x', 'x', 'x']
    id_title_map = sc.textFile("/user/ellery/missing_and_exisiting_for_top_50_langs.tsv.gz").map(get_parser(names))
    id_title_map = id_title_map.map(lambda x: (int(x['id'][1:]), x['name']))
    id_title_map = dict(id_title_map.join(candidates.map(lambda x: (x, x))).map(lambda x: (x[0], x[1][0])).collect())
    print "Got %d distinct items" % len(id_title_map)

    i=0
    for k,v in id_title_map.iteritems():
        print k, v
        if i==10:
            break 
        i+=1

    model, cv_results = cross_validation(training, validation, test, candidates, id_title_map, ranks, lambdas, numIters, alphas)
    pprint(cv_results)
    qe_results = qualitative_evaluation(model, candidates, id_title_map)
    sc.stop()
    pprint(cv_results)
    pprint(qe_results)

    


