#!/usr/bin/python

import re, codecs, sys, os, gzip

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DATA_DIR = os.environ['HOME'] + '/wikimedia/trunk/data/'

langs = {x.strip() for x in open(DATA_DIR + 'largest_wikipedias.txt')}

# This is the order of languages in which we pick a representative name for a concept.
lang_order = ['en', 'de', 'fr', 'es', 'pt', 'ca', 'it', 'nl', 'sv', 'da', 'pl', 'no', 'fi', 'cs', 
  'ro', 'hu', 'tr', 'eo', 'simple']
lang_order += list(langs - set(lang_order))

def find_representative_title(titles_for_concept):
  repr_title = ''
  for l in lang_order:
    if l in titles_for_concept:
      repr_title = l + ':' + titles_for_concept[l]
      break
  return repr_title

# Read input from data/missing_articles/interlanguage_links.tsv.
f = gzip.open(DATA_DIR + 'missing_articles/interlanguage_links.tsv.gz', 'rb')

prev_concept = None
langs_for_concept = set()
titles_for_concept = dict()

for line in codecs.getreader('utf8')(f):
  concept, lang, title = line.strip().split('\t')
  if (prev_concept is not None and concept != prev_concept):
    try:
      print '\t'.join([
        prev_concept,
        str(len(langs_for_concept)),
        str(len(langs - langs_for_concept)),
        find_representative_title(titles_for_concept),
        '|'.join(sorted(langs_for_concept)),
        '|'.join(sorted(langs - langs_for_concept)),
        '|'.join([titles_for_concept[l] for l in sorted(langs_for_concept)])
        ])
      langs_for_concept.clear()
      titles_for_concept.clear()
    except UnicodeEncodeError:
      print '------------- UnicodeEncodeError: ' + prev_concept
  prev_concept = concept
  if (lang in langs):
    langs_for_concept.add(lang)
    titles_for_concept[lang] = title

# Flush the last concept.
print '\t'.join([
  concept,
  str(len(langs_for_concept)),
  str(len(langs - langs_for_concept)),
  find_representative_title(titles_for_concept),
  ','.join(sorted(langs_for_concept)),
  ','.join(sorted(langs - langs_for_concept))
  ])
