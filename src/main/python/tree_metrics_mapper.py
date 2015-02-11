#!/usr/bin/python

import json, sys, errno, codecs, datetime
from pprint import pprint

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

def dfs(tree):
  metrics = dict()
  if 'children' not in tree:
    metrics['size'] = 1
    metrics['size_sum'] = 1
    metrics['size_sq_sum'] = 1
    metrics['num_leafs'] = 1
    metrics['depth_max'] = 0
    metrics['depth_sum'] = 0
    metrics['degree_max'] = 0
    metrics['degree_sum'] = 0
    metrics['timediff_sum'] = 0
    metrics['timediff_min'] = None
    metrics['timediff_max'] = None
    metrics['ambiguous_max'] = 1 if 'parent_ambiguous' in tree.keys() else 0
    metrics['ambiguous_sum'] = 1 if 'parent_ambiguous' in tree.keys() else 0
  else:
    k = len(tree['children'])
    dt = datetime.datetime.strptime(tree['dt'], DATE_FORMAT)
    metrics['size'] = 0
    metrics['size_sum'] = 0
    metrics['size_sq_sum'] = 0
    metrics['num_leafs'] = 0
    metrics['depth_max'] = 0
    metrics['depth_sum'] = 0
    metrics['degree_max'] = 0
    metrics['degree_sum'] = 0
    metrics['timediff_sum'] = 0
    metrics['timediff_min'] = float('inf')
    metrics['timediff_max'] = float('-inf')
    metrics['ambiguous_max'] = 0
    metrics['ambiguous_sum'] = 0
    for ch in tree['children']:
      child_metrics = dfs(ch)
      dt_child = datetime.datetime.strptime(ch['dt'], DATE_FORMAT)
      timediff = (dt_child - dt).total_seconds()
      metrics['size'] += child_metrics['size']
      metrics['size_sum'] += child_metrics['size_sum']
      metrics['size_sq_sum'] += child_metrics['size_sq_sum']
      metrics['num_leafs'] += child_metrics['num_leafs']
      metrics['depth_max'] = max(metrics['depth_max'], child_metrics['depth_max'])
      metrics['depth_sum'] += child_metrics['depth_sum']
      metrics['degree_max'] = max(metrics['degree_max'], child_metrics['degree_max'])
      metrics['degree_sum'] += child_metrics['degree_sum']
      metrics['timediff_sum'] += timediff
      metrics['timediff_min'] = min(metrics['timediff_min'], timediff)
      metrics['timediff_max'] = max(metrics['timediff_min'], timediff)
      metrics['ambiguous_max'] = max(metrics['ambiguous_max'], child_metrics['ambiguous_max'])
      metrics['ambiguous_sum'] += child_metrics['ambiguous_sum']
    metrics['size'] += 1
    metrics['size_sum'] += metrics['size']
    metrics['size_sq_sum'] += metrics['size'] * metrics['size']
    metrics['depth_max'] += 1
    metrics['depth_sum'] += metrics['depth_max']
    metrics['degree_sum'] += k
    metrics['degree_max'] = max(metrics['degree_max'], k)
    metrics['ambiguous_max'] = max(metrics['ambiguous_max'], 1 if 'parent_ambiguous' in tree.keys() else 0)
    metrics['ambiguous_sum'] += 1 if 'parent_ambiguous' in tree.keys() else 0
  return metrics

if __name__ == '__main__':

  key_list = None
  i = 0
  print '\t'.join([
    'size',
    'depth',
    'degree_max', 'degree_mean',
    'wiener_index',
    'timediff_min', 'timediff_max', 'timediff_mean',
    'ambiguous_max', 'ambiguous_mean'])
  for line in sys.stdin:
    i += 1
    try:
      tree = json.loads(line)
      metrics = dfs(tree)
      n = float(metrics['size'])
      n_internal = float(metrics['size'] - metrics['num_leafs'])
      print metrics['size'], '\t',
      print metrics['depth_max'], '\t',
      print metrics['degree_max'], '\t',
      print metrics['degree_sum'] / n_internal if n_internal > 0 else float('NaN'), '\t',
      print 2*n/(n-1) * (metrics['size_sum']/n - metrics['size_sq_sum']/(n*n)) if n > 1 else float('NaN'), '\t',
      print metrics['timediff_min'], '\t',
      print metrics['timediff_max'], '\t',
      print metrics['timediff_sum'] / (n-1) if n > 1 else float('NaN'), '\t',
      print metrics['ambiguous_max'], '\t',
      print metrics['ambiguous_sum'] / n, '\t',
      print ''
    except IOError as e:
      if e.errno == errno.EPIPE: break

try: sys.stdout.close()
except: pass
try: sys.stderr.close()
except: pass
