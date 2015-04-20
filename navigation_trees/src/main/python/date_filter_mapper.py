#!/usr/bin/python

# The results from the first run might contain trees from 02/2015.
# In order not to count those doubly, we filter them here.

import json, sys, errno, codecs
from pprint import pprint

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

if __name__ == '__main__':

  for line in sys.stdin:
    try:
      tree = json.loads(line)
      if (tree['dt'].startswith('2015-01')):
        print line
    except IOError as e:
      if e.errno == errno.EPIPE: pass

try: sys.stdout.close()
except: pass
try: sys.stderr.close()
except: pass
