#!/home/ellery/miniconda3/bin/python

import wikiclass
from revscoring.scorer_models import MLScorerModel
import requests
from collections import Counter
import time
      
model = MLScorerModel.load(open(
  "/home/west1/github/wikiclass/models/enwiki.wp10.rf.model", "rb"))

datadir = os.environ['HOME'] + '/wikimedia/hoaxes/data/speedyDeletionWiki/'

def get_prediction(model, title):
  url_template = 'https://fr.wikipedia.org/w/index.php?action=raw&title=%s'
  url = url_template % title
  response = requests.get(url)  
  if response.status_code != 200:
    return 0
  else:
    return wikiclass.score(model, response.text)

for f in os.listdir(datadir + 'nonhoax_markup_cleaned/'):
  if f.endswith(".html"):
    print f
    with open(datadir + 'nonhoax_markup_cleaned/' + f, 'r') as markup_file:
      markup = markup_file.read()
      print wikiclass.score(model, markup)
    out.close()
  else:
    continue
