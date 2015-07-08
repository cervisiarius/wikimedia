#!/usr/bin/python

import sys, errno, codecs, gzip, subprocess

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

if __name__ == '__main__':

  redirects = dict()

  #for line in codecs.getreader('utf8')(gzip.open('enwiki_20141008_redirects.tsv.gz')):
  cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/west1/wikipedia_redirects/enwiki_20141008_redirects.tsv"],
    stdout=subprocess.PIPE)
  for line in codecs.getreader('utf8')(cat.stdout):
    tokens = line.strip().split('\t', 2)
    try:
      redirects[tokens[0]] = tokens[1]
    except IndexError:
      # This happens for one line with a weird unicode character.
      pass

  last_key = None
  last_num_bytes = None
  pos = dict()
  for line in sys.stdin:
    # source, target, num_bytes, link positions (comma-separated).
    tokens = line.strip().split('\t')

    if (last_key is not None and tokens[0] != last_key):
      for target in pos:
        print '\t'.join([last_key, target, last_num_bytes, pos[target]])
      pos = dict()

    last_key = tokens[0]
    last_num_bytes = tokens[2]
    target = tokens[1]
    if target in redirects:
      target = redirects[target]
    pos[target] = pos[target] + ',' + tokens[3] if target in pos else tokens[3]

  # Process last.
  for target in pos.keys():
    print '\t'.join([last_key, target, last_num_bytes, pos[target]])
