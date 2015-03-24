#!/usr/bin/python

# Expects one command-line argument: a comma-separated list of languages.

import sys, os, codecs, gzip, math, random
from collections import deque

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

#langs = {x.strip() for x in open(DATA_DIR + 'largest_wikipedias.txt')}
langs = sys.argv[1].split(',')
existing = dict([(lang, deque()) for lang in langs])

for lang in langs:
  f = gzip.open(DATA_DIR + 'missing_articles/mids_per_language/mids_' + lang + '.txt.gz', 'rb')
  for line in f:
    existing[lang].append(line.strip())
  f.close()

# Sample articles that exist but haven't been edited by the user.
def sample_negatives(data_for_user, lang):
  ex = existing[lang]
  N = len(data_for_user)
  neg = set()
  while len(neg) < N:
    sample = ex[random.randint(0, len(ex)-1)]
    neg.add(sample)
  return neg

prev_uid = None
data_for_user = deque()
LOG2 = math.log(2)

i = 0
# First read all edits of the same user, then sample some articles that the user hasn't edited but
# that exist in the respective language, and output the data in Vowpal Wabbit's format.
for line in sys.stdin:
  lang, uid, user, mid, title, num_edits, bytes_added = line.strip().split('\t')
  if uid != prev_uid:
    for edit in data_for_user:
      print '{:f} |u {} |i {}'.format(math.log(1 + edit[2]) / LOG2, uid, edit[1])
    for nonedit in sample_negatives(data_for_user, lang):
      print '{:f} |u {} |i {}'.format(0, uid, nonedit)
    data_for_user.clear()
  data_for_user.append((lang, mid, int(num_edits), int(bytes_added)))
  prev_uid = uid
