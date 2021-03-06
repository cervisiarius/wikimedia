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
    'referer_article',
    'is_hoax_article',
    'is_hoax_referer',
    'http_status',
    'is_bot',
    'date',
    'uid'])
  for line in sys.stdin:
    pv = json.loads(line)
    ref_parsed = urlparse(pv['referer'])
    ref_domain = ref_parsed.netloc
    if ref_domain.startswith('www.'): ref_domain = ref_domain[4:]
    date = pv['dt'].split('T')[0]
    art = pv['uri_path'].split('/wiki/')[1]
    if ref_domain == 'en.wikipedia.org' and ref_parsed.path.startswith('/wiki/'):
      ref_art = ref_parsed.path.split('/wiki/')[1]
    else:
      ref_art = ''
    #art = urllib.unquote(pv['uri_path'].split('/wiki/')[1]).decode('utf8')
    print '\t'.join([
      art
      , pv['referer']
      , ref_domain
      , ref_art
      , '1' if 'hoax_in_url' in pv.keys() else '0'
      , '1' if 'hoax_in_referer' in pv.keys() else '0'
      , pv['http_status'],
      , '1' if 'is_bot' in pv.keys() else '0'
      , date
      , pv['uid']
      ])
