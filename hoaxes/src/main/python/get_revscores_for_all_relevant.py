#!/home/ellery/miniconda3/bin/python

import wikiclass
from revscoring.scorer_models import MLScorerModel
import requests
from collections import Counter
import time
import os

model = MLScorerModel.load(open(
  "/home/west1/github/wikiclass/models/enwiki.wp10.rf.model", "rb"))

datadir = os.environ['HOME'] + '/wikimedia/trunk/hoaxes/data/'

print('\t'.join(['title', 'Stub', 'B', 'C', 'FA', 'Start', 'GA']))
for f in os.listdir(datadir + 'all_relevant_article_creation_content'):
  if f.endswith(".txt"):
    with open(datadir + f, 'r') as markup_file:
      markup = markup_file.read()
      obj = wikiclass.score(model, markup)
      print('\t'.join([f, str(obj['probability']['Stub']),
        str(obj['probability']['B']), str(obj['probability']['C']), str(obj['probability']['FA']),
        str(obj['probability']['Start']), str(obj['probability']['GA'])]))
  else:
    continue
