#!/usr/bin/python

import json, codecs, sys, gzip

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

new_links = set()
with gzip.open('links_added_in_02-15_FILTERED.tsv.gz') as f:
  for line in f:
    tokens = line.split('\t')
    new_links.add((tokens[0], tokens[1]))

#new_links.add(('Main_Page', 'Witchcraft'))

def dfs(root, path_to_root):
  tuples = []
  l = len(path_to_root)
  if l >= 2:
    for i in range(0, l-1):
      # Skip nodes representing a search.
      if 'is_search' not in root:
        s, m, t = path_to_root[i], path_to_root[i+1], root['title']
        # All elements in a triple must be different; also, (s,t) must be a new link.
        if s != m and s != t and m != t: # and (s,t) in new_links:
          tuples = tuples + [(l-i, s, m, t)]
  if 'children' in root:
    for ch in root['children']:
      # Skip nodes representing a search.
      if 'is_search' not in root:
        p = path_to_root + [root['title']]
      else:
        p = path_to_root
      tuples = tuples + dfs(ch, p)
  return tuples

if __name__ == '__main__':

  for line in sys.stdin:
    try:
      obj = json.loads(line)
      tuples = dfs(obj, [])
      lengths = dict()
      # Find the minimum length for each (s,m,t) triple:
      for t in tuples:
        if t[1:] not in lengths or t[0] < lengths[t[1:]]:
          lengths[t[1:]] = t[0]
      # Consider only min lengths, and cast to set to discard duplicates.
      tuples = sorted(set([(lengths[t[1:]], t[1], t[2], t[3]) for t in tuples]))
      if len(tuples) > 0:
        tuples = [(obj['id'], str(t[0]), t[1], t[2], t[3]) for t in tuples]
        print '\n'.join(['\t'.join(t) for t in tuples])
    except ValueError:
      # Some lines contain corrupted JSON (unterminated lines).
      pass

  # j = """{
  # "title": "Battlefield_Earth_(film)",
  # "children": [
  #   {
  #     "children": [
  #       {
  #         "title": "List_of_box_office_bombs"
  #       }
  #     ],
  #     "title": "List_of_films_considered_the_worst"
  #   }
  # ]}"""
  # obj = json.loads(j)
  # obj['tuples'] = dfs(obj, [])
  # print json.dumps(obj)
