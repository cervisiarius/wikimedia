#!/usr/bin/python

# In this proof of concept, we only deal with concepts that have an English article and consider
# only pageview counts for these English articles.
# This is very rudimentary, e.g., no title normalization is done (URL-decoding and redirect
# resolution)

import re, codecs, sys, os, gzip
from collections import defaultdict

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

counts = defaultdict(int)

# Load the pageview counts.
# NB: No title normalization is done here!!!
f = gzip.open(DATA_DIR + 'pageview_counts/pageview_counts_enwiki.tsv.gz', 'rb')
for line in codecs.getreader('utf8')(f):
  title, count = line.split('\t')
  count = int(count)
  # Pages that were viewed less than 15 times get a count of 0.
  # TODO: Remove after testing.
  if count < 100:
    break
  else:
    counts[title.replace('_', ' ')] = count
f.close()

# Iteratate over missing articles and add count info.
f = gzip.open(DATA_DIR + 'missing_articles/missing_and_exisiting_for_top_50_langs.tsv.gz', 'rb')
for line in codecs.getreader('utf8')(f):
  tokens = line.split('\t')
  title = tokens[3].split(':')[1]
  tokens = tokens[0:4] + [str(counts[title])] + tokens[4:]
  print '\t'.join(tokens)
f.close()
