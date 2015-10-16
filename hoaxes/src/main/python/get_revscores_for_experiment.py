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

for f in os.listdir(datadir + 'nonhoax_markup_cleaned/'):
  if f.endswith(".html"):
    print(f)
    with open(datadir + 'nonhoax_markup_cleaned/' + f, 'r') as markup_file:
      markup = markup_file.read()
      obj = wikiclass.score(model, markup)
      print('{}\t{}\t{}\t{}\t{}\t{}' % obj['probability']['Stub'],
        obj['probability']['B'], obj['probability']['C'], obj['probability']['FA'],
        obj['probability']['Start'], obj['probability']['GA'])
  else:
    continue
