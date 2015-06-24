from scipy.io import mmread
import scipy
from pprint import pprint
from sklearn.preprocessing import normalize
from ConfigParser import SafeConfigParser
import collections
import time
import heapq
import json
import numpy as np
import pandas as pd


def split_contributions(contributions, k, l):
    assert (len(contributions['contributions']) >=(k+l) )
    train = {'uid' : contributions['uid'], 'uname' : contributions['uname'], 'contributions' : contributions['contributions'][-(k+l):-l]}
    test = {'uid' : contributions['uid'], 'uname' : contributions['uname'], 'contributions' : contributions['contributions'][-l:]}
    return train, test


def contribution_iter(contribution_file, k, l, m, min_bytes=100):
    assert(k+l <= m)
    with open(contribution_file) as f:
        for line in f:
            try:
                contributions = json.loads(line)
            except:
                print "decode error"
                continue
            #contributions['contributions'] = [x for x in contributions['contributions'] if int(x['bytes_added']) >= min_bytes]
            if len(contributions['contributions']) >= (m):
                yield split_contributions(contributions, k, l)          



def get_translation_maps(dict_file):
    id2sname = {}
    id2importance = {}
    with open(dict_file) as f:
        for line in f:
            row = line.strip().split('\t')
            item = row[0]
            sname = row[1]
            importance = 1.0 #math.log(float(row[2]))           
            id2sname[item] = sname
            id2importance[item] = importance
            
    return id2sname, id2importance


def get_universal_maps(dict_file):
    id2index = {}
    index2id = []
    id2name = {}
    
    with open(dict_file) as f:
        for index, identifier in enumerate(f):
            identifiers = identifier.strip().split('|')
            item_id  = identifiers[2]
            name = identifiers[1]
            
            id2name[item_id] = name
            id2index[item_id] = index
            index2id.append(item_id)
    return id2index, np.array(index2id), id2name


def reduce_and_reweight(M, id2importance, id2index, index2id):
    num_docs = M.shape[0]
    M = M.transpose()
    diagonal = [ id2importance[index2id[i]] if index2id[i] in id2importance else 0.0 for i in range(num_docs)]
    diagonal = np.array(diagonal)
    assert(np.count_nonzero(np.isnan(diagonal))==0)
    R = scipy.sparse.diags(diagonal, 0)
    M = M*R
    return M.transpose().tocsr()


def get_average_interest_vector(M, id2index, item_target_importance, contributions):
    contributions = [d for d in contributions['contributions'] if d['id'] in id2index]
    if len(contributions) == 0:
        return np.zeros(M.shape[1])
    item_indices = [id2index[x['id']] for x in contributions]
    interest_matrix = M[item_indices].toarray()
    interest_vector = np.sum(interest_matrix, axis=0)
    return interest_vector / np.linalg.norm(interest_vector)


def get_weighted_average_interest_vector(M, id2index, item_target_importance, contributions):
    contributions = [d for d in contributions['contributions'] if d['id'] in id2index]
    if len(contributions) == 0:
        return np.zeros(M.shape[1])
    weights = np.log(np.array([max(2.0, float(d['bytes_added'])) for d in contributions]))
    item_indices = [id2index[x['id']] for x in contributions]
    interest_matrix = M[item_indices].toarray()
    interest_vector = interest_matrix.T.dot(weights)
    return interest_vector / np.linalg.norm(interest_vector)

def get_weighted_mediod_interest_vector(M, id2index, item_target_importance, contributions):
    contributions = [d for d in contributions['contributions'] if d['id'] in id2index]
    if len(contributions) == 0:
        return np.zeros(M.shape[1])
    weights = np.log(np.array([max(2.0, float(d['bytes_added'])) for d in contributions]))
    item_indices = [id2index[x['id']] for x in contributions]
    interest_matrix = M[item_indices].toarray()
    average_vector = interest_matrix.T.dot(weights)
    idx = np.array([np.linalg.norm(v) for v in interest_matrix-average_vector]).argmin()
    mediod = interest_matrix[idx]
    return mediod / np.linalg.norm(mediod)
    
    
def compute_AP(ranking, items):
    # precision is 0 if item is not ranked
    reciprocal_ranks = 0.0
    num_test_items_in_ranking = 0.0
    for i, t in enumerate(ranking):
        if t[0] in items:
            reciprocal_ranks += 1.0 / (i+1)
            num_test_items_in_ranking+=1
    return reciprocal_ranks / len(items), num_test_items_in_ranking

    
def recommend_and_eval(train, test, rdata, verbose = False):

    test_items = [d['id'] for d in test['contributions']]
    MR = rdata['MR']
    MI = rdata['MI']
    id2index = rdata['id2index']
    index2id = rdata['index2id']
    id2uname = rdata['id2uname']
    item_target_importance = rdata['item_target_importance']
    num_to_rank = rdata['num_to_rank']
    min_score = rdata['min_score']
    get_interest_vector = rdata['get_interest_vector']
    time1 = time.time()
    ranking = recommend(MI, MR, id2index, index2id, item_target_importance, train, get_interest_vector, num_to_rank, min_score)
    time2 = time.time()
    #print 'Getting Recommendation took %0.3f seconds' % (time2-time1)
    AP, num_test_items_in_ranking = compute_AP(ranking, test_items )
    if verbose:
        k = 20
        print "\n\n#####################  Example ##################" 
        print "Ranked %d out of %d test items" % (num_test_items_in_ranking, len(test_items))
        train_items = [d['id'] for d in train['contributions']]
        train_names = [id2uname[x] for x in train_items if x in id2uname]
        test_names = [id2uname[x] for x in test_items if x in id2uname]
        ranking_names = [(id2uname[x[0]], x[1]) for x in ranking[:k]]
        print "\nTraining Articles"
        pprint(list(train_names)[:k])
        print "\nTest Articles"
        pprint(list(test_names)[:k])
        
        print "\nTop Recommendations"
        pprint(ranking_names)
        
        print "\nAP: %f" %  AP
    
    return AP, num_test_items_in_ranking / len(test_items), len(ranking)
   

def recommend(MI, MR, id2index, index2id, item_target_importance, contributions, get_interest_vector, num_to_rank, min_score):
    interest_vector = get_interest_vector(MI, id2index, item_target_importance, contributions)
    scores = MR.dot(interest_vector)
    non_zero_indices = np.where(scores > min_score)
    ranking = np.argsort(-scores[non_zero_indices])[:num_to_rank]
    return zip(index2id[non_zero_indices][ranking], scores[non_zero_indices][ranking])


def print_results(i, MAP, FTIR, RL, condition = ''):
    n = float(i+1)
    MAP = MAP/n
    FTIR = FTIR/n
    RL = RL/n
    m = "%s MAP: %f MFTIR: %f MRL: %f"
    print m % (condition, MAP, FTIR, RL)



def recommend_and_eval_all(contribution_iter, rdata, num_examples, verbose = False):
    num_examples = num_examples-1                    
    MAP = 0.0
    avergage_fraction_of_test_items_ranked = 0.0
    avergage_ranking_length = 0.0
    MAPs = []

    for i, (train, test) in enumerate(contribution_iter):
        time1 = time.time()
        AP, fraction_of_test_items_ranked, ranking_length = recommend_and_eval(train, test, rdata, verbose = verbose)
        time2 = time.time()
        #print 'Getting Recommendation and eval took %0.3f seconds' % (time2-time1)
        MAP += AP
        avergage_fraction_of_test_items_ranked += fraction_of_test_items_ranked
        avergage_ranking_length += ranking_length
        
        if not verbose and (i+1) % 500 ==0:
            condition = 'i: %d K: %d f: %s ' % (i, len(train['contributions']), rdata['get_interest_vector'].__name__)
            print_results(i, MAP, avergage_fraction_of_test_items_ranked, avergage_ranking_length, condition = condition)
            MAPs.append(MAP/ float(i+1))

        if i==num_examples:
            break
                
    print "Evaluated %d editors" % (i+1)
    print_results(i, MAP, avergage_fraction_of_test_items_ranked, avergage_ranking_length)
    return MAPs
    