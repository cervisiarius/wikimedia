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
    tokens = line.split('\t', 2)
    try:
      redirects[tokens[0]] = tokens[1]
    except IndexError:
      # This happens for one line with a weird unicode character.
      pass

  for line in sys.stdin:
    # target (as reduce key), source, target, num_bytes, link positions (comma-separated).
    tokens = line.split('\t')
    if len(tokens) >= 3:
      try:
        tokens[2] = redirects[tokens[2]]
      except:
        pass
      print '\t'.join(tokens[1:])
