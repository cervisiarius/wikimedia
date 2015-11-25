#!/usr/bin/python

import codecs, sys

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

if __name__ == '__main__':

  old_uid = None
  paths_for_old_uid = []

  for line in sys.stdin:
    line = line.strip()
    # TODO: We should be smarter here: discard path, but still handle uid correctly, if there are too many tabs.
    try:
      uid, path = line.split('\t')[0:2]
      if old_uid is not None and uid != old_uid:
        p_old = ''
        for p in sorted(paths_for_old_uid, reverse=True):
          # If the current path is a prefix of the previous one, discard it.
          # Since paths are sorted in decreasing lexicographical order, this keeps only maximal
          # paths for the same user.
          if p_old == p or p_old.startswith(p + '|'):
            pass
          else:
            print '%s\t%s' % (old_uid, p)
          p_old = p
        del paths_for_old_uid[:]
      old_uid = uid
    except ValueError:
      sys.stderr.write('@@@@@' + line + '#####\n')
    paths_for_old_uid.append(path)

  # Output the last entry.
  p_old = ''
  for p in sorted(paths_for_old_uid, reverse=True):
    if p_old == p or p_old.startswith(p + '|'):
      pass
    else:
      print '%s\t%s' % (old_uid, p)
    p_old = p
