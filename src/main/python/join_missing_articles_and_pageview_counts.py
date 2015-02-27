#!/usr/bin/python

# In this proof of concept, we only deal with concepts that have an English article and consider
# only pageview counts for these English articles.

import re, codecs, sys, os, gzip
from collections import defaultdict

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

counts = defaultdict(int)

# Read input from data/missing_articles/interlanguage_links.tsv.
f = gzip.open(DATA_DIR + 'pageview_counts/pageview_counts_enwiki.tsv.gz', 'rb')

for line in codecs.getreader('utf8')(f):
  title, count = line.strip().split('\t')
  
