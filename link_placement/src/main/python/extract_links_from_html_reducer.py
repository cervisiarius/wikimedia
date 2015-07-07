#!/usr/bin/python

import sys, errno, codecs, gzip, subprocess

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

if __name__ == '__main__':

  redirects = dict()

  #f = gzip.open('enwiki_20141008_redirects.tsv.gz')
  cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/west1/wikipedia_redirects/enwiki_20141008_redirects.tsv"],
    stdout=subprocess.PIPE)
  for line in codecs.getreader('utf8')(cat.stdout):
    tokens = line.strip().split('\t', 2)
    try:
      redirects[tokens[0]] = tokens[1]
    except IndexError:
      # This happens for one line with a weird unicode character.
      pass

  for line in sys.stdin:
    # source, target, num_bytes, link positions (comma-separated).
    tokens = line.strip().split('\t')
    if len(tokens) >= 4:
      try:
        print '\t'.join(tokens[0:1] + [redirects[tokens[1]]] + tokens[1:4])
      except:
        print '\t'.join(tokens[0:2] + [''] + tokens[2:4])
