#!/usr/bin/python

# Expects two command-line argument: a comma-separated list of languages and the number of users
# for whom to make testexamples.

import sys, os, codecs, gzip, math, random
from collections import deque

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

langs = sys.argv[1].split(',')
num = int(sys.argv[2])

#langs = {x.strip() for x in open(DATA_DIR + 'largest_wikipedias.txt')}
existing = dict([(lang, deque()) for lang in langs])

for lang in langs:
  f = gzip.open(DATA_DIR + 'missing_articles/mids_per_language/mids_' + lang + '.txt.gz', 'rb')
  for line in f:
    mid, title = line.strip().split('\t')
    existing[lang].append(mid)
  f.close()

# Read the top 1k.
top = dict([(lang, dict()) for lang in langs])
f = open(DATA_DIR + 'top_wikidata_entities/top_10k_wikidata_entities.tsv', 'rb')
i = 0
for line in f:
  i += 1
  # Skip header.
  if i == 1: continue
  importance, mid, cat, title = line.strip().split('\t')
  importance = int(importance)
  for lang in langs:
    if importance > 0 and mid in existing[lang]:
      top[lang][mid] = title.replace(' ', '_')
f.close()

# Sample articles that exist but haven't been edited by the user.
def make_testing_examples(edited, lang):
  return set(top[lang].keys()) - edited

prev_uid = None
edited = set()

i = 0
for line in sys.stdin:
  lang, uid, user, mid, title, num_edits, bytes_added = line.strip().split('\t')
  if uid != prev_uid:
    for nonedit in make_testing_examples(edited, lang):
      print '{:f} |u {} |i {} |t {}'.format(0, uid, nonedit, top[lang][nonedit])
    edited.clear()
    i += 1
    if i == num: break
  edited.add(mid)
  prev_uid = uid
