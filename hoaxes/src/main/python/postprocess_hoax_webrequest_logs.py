import json, sys, urllib, codecs
from urlparse import urlparse

# We want to print unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)

if __name__ == '__main__':

  print '\t'.join([
    'article',
    'referer',
    'referer_domain',
    'is_hoax_article',
    'is_hoax_referer',
    'date',
    'uid'])
  for line in sys.stdin:
    pv = json.loads(line)
    ref_domain = urlparse(pv['referer']).netloc
    if ref_domain.startswith('www.'): ref_domain = ref_domain[4:]
    date = pv['dt'].split('T')[0]
    art = urllib.unquote(pv['uri_path'].split('/wiki/')[1])#.decode('utf8')
    print '\t'.join([
      art
      , pv['referer']
      , ref_domain
      , '1' if 'hoax_in_url' in pv.keys() else '0'
      , '1' if 'hoax_in_referer' in pv.keys() else '0'
      , date
      , pv['uid']
      ])
