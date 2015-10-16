#!/home/ellery/miniconda3/bin/python

import wikiclass
from revscoring.scorer_models import MLScorerModel
import requests
from collections import Counter
import time
import os, json
      
model = MLScorerModel.load(open(
  "/home/west1/github/wikiclass/models/enwiki.wp10.rf.model", "rb"))

datadir = os.environ['HOME'] + '/wikimedia/trunk/hoaxes/data/speedyDeletionWiki/'

print('\t'.join(['type', 'title', 'Stub', 'B', 'C', 'FA', 'Start', 'GA']))
for t in ['hoax', 'nonhoax']:
  subdir = 'markup_cleaned/' if t == 'hoax' else 'nonhoax_markup_cleaned/'
  for f in os.listdir(datadir + subdir):
    if f.endswith(".html"):
      with open(datadir + subdir + f, 'r') as markup_file:
        markup = markup_file.read()
        obj = wikiclass.score(model, markup)
        print('\t'.join([t, f, str(obj['probability']['Stub']),
          str(obj['probability']['B']), str(obj['probability']['C']), str(obj['probability']['FA']),
          str(obj['probability']['Start']), str(obj['probability']['GA'])]))
    else:
      continue
