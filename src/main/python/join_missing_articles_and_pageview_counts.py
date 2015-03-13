#!/usr/bin/python

# In this proof of concept, we only deal with concepts that have an English article and consider
# only pageview counts for these English articles.
# This is very rudimentary, e.g., no title normalization is done (URL-decoding and redirect
# resolution)

import codecs, sys, os, gzip, urllib
from collections import defaultdict

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

counts = defaultdict(int)

# Load the pageview counts.
# NB: No redirect resolution is done here!!!
f = gzip.open(DATA_DIR + 'pageview_counts/pageview_counts_enwiki.tsv.gz', 'rb')
for line in codecs.getreader('utf8')(f):
  title, count = line.split('\t')
  try:
    title = urllib.unquote(title)
  except UnicodeEncodeError:
    pass
  title = title.replace('_', ' ')
  try:
    count = int(count)
    # Pages that were viewed less than a minimum number of times get a count of 0.
    if count < 5: break
  except ValueError:
    count = 0
  counts[title] += count
f.close()

# Iteratate over missing articles and add count info.
f = gzip.open(DATA_DIR + 'missing_articles/missing_and_exisiting_for_top_50_langs.tsv.gz', 'rb')
for line in codecs.getreader('utf8')(f):
  tokens = line.split('\t', 1)
  try:
    lang, title = tokens[3].split(':')
    if lang == 'en':
      count = counts[title]
    else:
      count = 0
  except IndexError:
    # This happens if the concept has no article in any of the 50 languages considered.
    count = 0
  tokens = tokens[0:4] + [str(count)] + tokens[4:]
  print '\t'.join(tokens)
f.close()
