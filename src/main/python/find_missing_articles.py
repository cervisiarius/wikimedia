#!/usr/bin/python

# Read input from /lfs/1/data/wikidumps/wikidatawiki-20150113-pages-articles.xml.bz2

import re, codecs, sys, os

#sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

langs = {x.strip() for x in open(DATA_DIR + 'largest_wikipedias.txt')}

#f = open(DATA_DIR + 'missing_articles/interwiki_links.tsv')
f = sys.stdin

prev_concept = None
langs_for_concept = set()
en_title = None

for line in codecs.getreader('utf8')(f):
  concept, lang, title = line.strip().split('\t')
  if (prev_concept is not None and concept != prev_concept):
    try:
      print '\t'.join([
        prev_concept,
        str(len(langs_for_concept)),
        str(len(langs - langs_for_concept)),
        en_title if en_title is not None else '',
        ','.join(sorted(langs_for_concept)),
        ','.join(sorted(langs - langs_for_concept))
        ])
      langs_for_concept.clear()
      en_title = None
    except UnicodeEncodeError:
      print '------------- UnicodeEncodeError: ' + prev_concept
  prev_concept = concept
  if (lang in langs):
    langs_for_concept.add(lang)
  if (lang == 'en'):
    en_title = title

# Flush the last concept.
print '{}\t{}'.format(concept, ','.join(sorted(langs - langs_for_concept)))
