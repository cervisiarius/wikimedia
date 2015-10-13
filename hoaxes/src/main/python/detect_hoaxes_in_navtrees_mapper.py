import json, sys, urllib, codecs
from urlparse import urlparse

# We want to print unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

with open('titles.txt') as f:
  hoaxes = set([l.strip() for l in f.readlines()])

def bfs(root):
  uid = root['id'].split('_')[1]
  ref_parsed = urlparse(root['referer'])
  ref_domain = ref_parsed.netloc
  if ref_domain.startswith('www.'): ref_domain = ref_domain[4:]
  date = root['dt'].split('T')[0]
  if root['title'] in hoaxes:
    print u'\t'.join([root['title'],
      root['referer'],
      ref_domain,
      u'',
      u'1',
      u'0',
      date,
      uid])
  queue = [root]
  while len(queue) > 0:
    node = queue[0]
    queue = queue[1:]
    parent = node['title']
    if 'children' in node:
      for ch in node['children']:
        if not('is_search' in ch):
          queue = queue + [ch]
          hoax_in_url = u'1' if ch['title'] in hoaxes else u'0'
          hoax_in_ref = u'1' if parent in hoaxes else u'0'
          if hoax_in_url==u'1' or hoax_in_ref==u'1':
            print u'\t'.join([ch['title'],
              u'',
              u'en.wikipedia.org',
              parent,
              hoax_in_url,
              hoax_in_ref,
              date,
              uid])

if __name__ == '__main__':

  for line in sys.stdin:
    try:
      tree = json.loads(line)
      bfs(tree)
    except:
      pass
