import urllib2, urllib
import json
from pprint import pprint
import copy
from scipy.io import mmread
from ConfigParser import SafeConfigParser
import numpy as np
import pandas as pd
import time
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import random
from cvxopt import matrix, solvers,spmatrix
from eval_util import reduce_and_reweight, recommend, get_weighted_average_interest_vector




def contribution_iter(contributions):
    """
    Takes an iterable of json encoded editor contribution
    histories and decodes them
    """
    for c in contributions:
        try:
            contributions = json.loads(c)
        except:
            print "decode error"
            continue
        contributions['contributions'] = contributions['contributions']
        yield contributions    
        
def get_affinities(M,it, max_articles_per_editor, min_score, id2importance, id2index, index2id, w = 15):
    """
    Returns a dict mapping from editors to editor information dicts. Editor information includes contribution
    history of length w and editor-article affinities
    """
    MI = M
    contributions = {}
    MR = reduce_and_reweight(M, id2importance, id2index, index2id)
    for i, contribution in enumerate(it):
        contribution['contributions'] = [d for d in contribution['contributions'] if d['id'] in id2index][-w:]
        contribution['affinities'] = recommend(MI, MR, id2index, index2id, None, contribution, get_weighted_average_interest_vector, max_articles_per_editor, min_score)
        contributions[contribution['uname']] = contribution
    return contributions


def get_matching_data_structures(contributions):
    """
    Utility function for generating data structures required for lp mathcing
    """
    editor_ids = list(contributions.keys())
    article_ids = []
    affinities = {}
    for uname, c in contributions.iteritems():
        for x in c['affinities']:
            article_ids.append(x[0])
            edge = (uname, x[0])
            affinities[edge] = x[1]
    article_ids = list(set(article_ids))
    print ('Numer of unique articles: %d' % len(article_ids))
    print "number of editors %d" % len(editor_ids)
    
    return sorted(editor_ids), sorted(article_ids), affinities




def lp_match(editor_ids, article_ids, affinities, k ):
    
    assert(len(set(editor_ids).intersection(set(article_ids))) == 0)
    node2idx = {}
    idx2node = []
    b = []
    i = 0
    for ed in editor_ids:
        node2idx[ed] = i
        idx2node.append(ed)
        b.append(k)
        i+=1

    for a in article_ids:
        node2idx[a] = i
        idx2node.append(a)
        b.append(1.0)
        i+=1

    idx2edge = []
    c = []


    j = 0
    for e in affinities.keys():
        idx2edge.append(e)
        j+=1
        c.append(affinities[e])

    rows = []
    cols = []
    values = []
    
    print len(editor_ids) + len(article_ids), 'nodes'
    print len(affinities), 'edges'
    
    
    for j, e in enumerate(idx2edge):
        rows.append(node2idx[e[0]])
        cols.append(j)
        values.append(1.0)
        
        rows.append(node2idx[e[1]])
        cols.append(j)
        values.append(1.0)
        
    num_nodes = len(idx2node)
    num_edges = len(idx2edge)
    
    for e in idx2edge:
        b.append(1.0)

    
    for i in range(num_nodes, num_nodes + num_edges):
        rows.append(i)
        cols.append(i-num_nodes)
        values.append(1.0)
        
    for e in idx2edge:
        b.append(0.0)

    for i in range(num_nodes + num_edges, num_nodes + 2*num_edges):
        rows.append(i)
        cols.append(i-num_nodes - num_edges)
        values.append(-1.0)
    
    
    A =  spmatrix(values, rows, cols)
    print 'Starting Optimization'
    t1 = time.time()
    sol = solvers.lp(matrix(-1*np.array(c)),A,matrix(b))
    #sol = cvxopt.solvers.conelp(matrix(-1*np.array(c)),A,matrix(b), kktsolver="chol")
    t2 = time.time()
    print ('Optimization took %d seconds' % (t2-t1))
    non_zero, _ = np.where(np.round(sol['x']) ==1.0)
    objective = np.array(c).dot(sol['x']) / float(len(non_zero))
    print ("Objective %f " % objective )
    d = pd.DataFrame(idx2edge)
    d = d.ix[non_zero]
    print "Number of Duplicates: %d" % (len(non_zero) - d.shape[0])
    return objective[0], d


def greedy_match(contributions, k):
    t1 = time.time()
    matching = []
    used = set()
    obj = 0.0
    for i in range(k):
        for editor, c in contributions.iteritems():
            for entity, score in c['affinities']:
                if entity not in used:
                    used.add(entity)
                    matching.append((editor, entity, score))
                    obj += score
                    break
    obj = obj / float(len(matching))
    d = pd.DataFrame(matching)
    t2 = time.time()
    print ('Optimization took %d seconds' % (t2-t1))
    
    return obj, d


def get_thumbnail_url(s, title):
    template = "http://%s.wikipedia.org/w/api.php?action=query&format=json&prop=pageimages|pageterms&piprop=thumbnail&&pithumbsize=250&pilimit=10&redirects=true&titles=%s"
    url = template % (s, urllib.quote_plus(title.encode('utf8')))
    response = urllib2.urlopen(url)
    data = json.load(response)  
    try:
        url = data['query']['pages'].values()[0]['thumbnail']['source']
        return url
    except:
        return ''
    
def get_ct_url(campaign, s,t,title):
    template = "https://%(t)s.wikipedia.org/wiki/Special:ContentTranslation?campaign=%(campaign)s&to=%(t)s&from=%(s)s&page=%(title)s"
    return template % {"s":s, "t":t, "title":title, "campaign":campaign }


def pprint_rec(obj):
    print "User: %s\n" % obj['user']
    print "History:\n"
    for item in obj['history'][-15:]:
        print item['page_title']
    print "\nRecommendations:\n"
    for rec in obj['recommendations']:
        print rec['title'], rec['score']
    print "\n\n"

def get_json(d_user, d_rec, id2sname, contributions, campaign, s, t, k):
    objs = []
    num_failures = 0
    for i, g in enumerate(d_rec.groupby(0)):
        dm = copy.copy(g[1])
        dm.columns = ['uname', 'id', 'score']
        dm['name'] = dm['id'].apply(lambda x: id2sname[x])
        uname = dm['uname'][dm.index[0]]
        obj = {'user': uname, 'email':d_user.loc[uname]['user_email'], 'history': contributions[uname]['contributions']}

        dm['name'] =  dm['name'].apply(lambda x: x.replace(" ", "_"))
        #dm['image_url'] = dm['name'].apply(lambda x: get_thumbnail_url(s, x))
        dm['ct_url'] = dm['name'].apply(lambda x: get_ct_url(campaign, s,t,x))

        recs = []

        for i in range(dm.shape[0]):
            r = dm.iloc[i]
            rec = {
                'title': r['name'],
                'id': r['id'],
                #'image_url': r['image_url'],
                'ct_url': r['ct_url'],
                'score' : r['score'] 
            }
            recs.append(rec)
        obj['recommendations'] = recs
        if len(recs) == k:
            objs.append(obj)
        else:
            num_failures+=1
    print "There were %d users without %d recs" % (num_failures, k)
    return objs