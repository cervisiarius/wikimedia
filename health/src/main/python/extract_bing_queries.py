#!/usr/bin/python

# NB: UNUSED. We do everything within Pig now: aggregate_bing_queries.pig

import codecs, sys, re, json, urllib

# We want to read and write unicode.
sys.stdin = codecs.getwriter('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

regex = re.compile(r'https?://([^/]+\.)?bing\.com/search\?.*?q=(.*?)(&|$)')

for line in sys.stdin:
  try:
    tree = json.loads(line)
    title = tree['title']
    ref = tree['referer']
    m = regex.match(ref)
    if m:
      #q = urllib.unquote(m.group(2).replace('+', ' ')).decode('utf8')
      q = m.group(2).replace('+', ' ')
      print '{}\t{}'.format(title, q)
  except:
    pass
