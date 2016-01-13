#!/usr/bin/python

# This script takes the output of sort_revision_table_and_add_first_flag.pig as input.
# Write output to hdfs:/user/west1/enwiki_metadata/num_creator_edits_before_creation.tsv.

import sys, codecs

# We want to print unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

if __name__ == '__main__':

  prev_user = None
  count = 0

  for line in sys.stdin:
    count += 1
    tokens = line.strip().split('\t', 11)
    try:
      (user, is_first) = (tokens[4], True if tokens[6] == '1' else False)
      if user != prev_user:
        count = 1
      if is_first:
        print '\t'.join(tokens[:6] + [str(count-1)])
      prev_user = user
    except IndexError:
      pass
