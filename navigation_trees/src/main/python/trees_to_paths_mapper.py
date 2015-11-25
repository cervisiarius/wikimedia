#!/usr/bin/python

import json, codecs, sys

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

def dfs(root, path_to_root):
  if 'children' in root:
    paths = []
    for ch in root['children']:
      # Skip nodes representing a search.
      # NB: This means that paths may contain transitions that don't correspond to links, e.g.,
      # (A, B, SEARCH, C) will appear as (A, B, C).
      if 'is_search' not in root:
        p = path_to_root + [root['title']]
      else:
        p = path_to_root
      # The recursive call.
      paths = paths + dfs(ch, p)
    return paths
  else:
    # Again, skip nodes representing a search.
    if 'is_search' not in root:
      return [path_to_root + [root['title']]]
    else:
      return [path_to_root]

if __name__ == '__main__':

  for line in sys.stdin:
    try:
      obj = json.loads(line)
      uid = '_'.join(obj['id'].split('_')[0:2])
      path_strings = ['|'.join(p) for p in dfs(obj, [])]
      # Cast to set to discard duplicates.
      # Some paths are empty (if the root is a search); ignore those.
      path_strings = filter(lambda p: len(p) > 0, set(path_strings))
      if len(path_strings) > 0:
        print '\n'.join(uid + '\t' + p for p in path_strings)
    except ValueError:
      # Some lines contain corrupted JSON (unterminated lines).
      pass
