from scipy.io import mmread
import scipy
from pprint import pprint
from sklearn.preprocessing import normalize
from ConfigParser import SafeConfigParser
import numpy as np
import time
import multiprocessing
import time
import copy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import os,sys,inspect
from eval_util import *
import eval_util
import json
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--m', required = True, type = int, help='min edit size' )
parser.add_argument('--ws', required = True, help='eg 1,2,4,8' )
parser.add_argument('--n', required = True, type = int, help='num editors' )
parser.add_argument('--c', required = True, type = int, help='num cores' )
parser.add_argument('--fs', required = True, help='functions to test' )
parser.add_argument('--exp', required = True, help='name of experiment' )


cmd_args = parser.parse_args()
m = cmd_args.m
ws = [int(w) for w in cmd_args.ws.split(',')]
n = cmd_args.n


methodToCall = getattr(foo, 'bar')


interest_functions = [ getattr(eval_util, f) for f in cmd_args.fs.split(',')]



universal_dir = 'en_lda'
translation_dir = 'en_lda'
config = '/home/ellery/wikimedia/missing_articles/missing_articles.ini'

cp = SafeConfigParser()
cp.read(config)

universal_base_dir = os.path.join(cp.get('general', 'local_data_dir'), universal_dir)
universal_hadoop_base_dir = os.path.join(cp.get('general', 'hadoop_data_dir'), universal_dir)

translation_base_dir = os.path.join(cp.get('general', 'local_data_dir'), translation_dir)
translation_hadoop_base_dir = os.path.join(cp.get('general', 'hadoop_data_dir'), translation_dir)


# only when doing translation
if universal_dir != translation_dir:
    translation_dict_file = os.path.join(translation_base_dir, cp.get('missing', 'ranked_missing_items'))
    id2sname, id2importance = get_translation_maps(translation_dict_file)

universal_dict_file = os.path.join(universal_base_dir, cp.get('LDA', 'article2index'))
id2index, index2id, id2uname = get_universal_maps(universal_dict_file)


mm_file = os.path.join(universal_base_dir, cp.get('LDA', 'doc2topic'))
f = open(mm_file)
M = mmread(f).tocsr()
M = normalize(M, norm='l2', axis=1)

rdata = {}
rdata['MI'] = M
if universal_dir == translation_dir:
    rdata['MR'] = M 
else:
    rdata['MR'] = reduce_and_reweight(M, id2importance, id2index, index2id)
rdata['id2index'] = id2index
rdata['index2id'] = index2id 
rdata['id2uname'] = id2uname 
rdata['item_target_importance'] = None 
rdata['num_to_rank'] = 10000
rdata['min_score'] = 0.5


contribution_file = os.path.join(translation_base_dir, cp.get('eval', 'train'))  


args = {}
args['m'] = m
args['l'] = 1
args['n'] = n

args_list = []


for w in ws:
    for f in interest_functions:
        argsd = copy.deepcopy(args)
        argsd['k'] = w
        argsd['get_interest_vector'] = f
        args_list.append(argsd)


def mp_worker(args):
    m = args['m']
    k = args['k']
    l = args['l']
    n = args['n']
    f = args['get_interest_vector']
    rdata['get_interest_vector'] = f
    MAPs = recommend_and_eval_all(contribution_iter(contribution_file, k, l, m), rdata, n, verbose = False)
    res = {}
    res['k'] = k
    res['f'] = f.__name__
    res['MAPs'] = MAPs
    return res


    
p = multiprocessing.Pool(cmd_args.c)
time1 = time.time()
results = p.map(mp_worker, args_list)
time2 = time.time()
print ('Time: ', (time2-time1) )
pprint(results)


"""
fig = plt.figure(figsize=(18,8))
for d in results:
    plt.plot(d['MAPs'])
plt.savefig('trajectories_all.pdf')

fig = plt.figure(figsize=(18,8))
for d in results:
    plt.plot(d['MAPs'][-50:])
plt.savefig('trajectories_50.pdf')

fig = plt.figure(figsize=(18,8))
for d in results:
    plt.plot(d['MAPs'][-10:])
plt.savefig('trajectories_10.pdf')
"""

with open('cv_results_%s.txt' % cmd_args.exp, 'w') as outfile:
    json.dump(results, outfile)






