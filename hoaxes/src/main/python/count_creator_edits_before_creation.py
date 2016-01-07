#!/usr/bin/python

# This script takes the output of sort_revision_table_and_add_first_flag.pig as input.

import sys, codecs

# We want to print unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

if __name__ == '__main__':

  prev_uid = None
  count = 0

  for line in sys.stdin:
    line = line.strip()
    tokens = line.split('\t', 6)
    (uid, is_first) = (tokens[3], True if tokens[6] == '1' else False)
    count += 1
    if uid != prev_uid:
      count = 1
    if is_first:
      print '\t'.join(tokens[:6] + [str(count-1)])
    prev_uid = uid
