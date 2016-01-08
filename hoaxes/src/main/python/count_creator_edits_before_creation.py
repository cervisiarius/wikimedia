#!/usr/bin/python

# This script takes the output of sort_revision_table_and_add_first_flag.pig as input.
# Write output to ~/repo/hoaxes/data/num_creator_edits_before_creation.tsv.gz.

import sys, codecs

# We want to print unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

if __name__ == '__main__':

  prev_user = None
  count = 0

  for line in sys.stdin:
    line = line.strip()
    tokens = line.split('\t', 6)
    (user, is_first) = (tokens[4], True if tokens[6] == '1' else False)
    count += 1
    if user != prev_user:
      count = 1
    if is_first:
      print '\t'.join(tokens[:6] + [str(count-1)])
    prev_user = user
