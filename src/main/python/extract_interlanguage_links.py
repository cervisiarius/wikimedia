#!/usr/bin/python

# Read input from /lfs/1/data/wikidumps/wikidatawiki-20150113-pages-articles.xml.bz2

import json, re, codecs, sys, HTMLParser

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

JSON_PATTERN = re.compile(r'^\s*<text xml:space="preserve">(\{&quot;type&quot;:&quot;item&quot;,&quot;id&quot;:&quot;Q.*&quot;,&quot;labels&quot;:.*)</text>')
WIKI_PATTERN = re.compile(r'^(.*)wiki$')
PARSER = HTMLParser.HTMLParser()

for line in sys.stdin:
	match = JSON_PATTERN.match(line)
	if match:
		line = PARSER.unescape(match.group(1))
		obj = json.loads(line)
		links = obj['sitelinks']
		for wiki in sorted(links.keys()):
			m = WIKI_PATTERN.match(wiki)
			if m:
				lang = m.group(1)
				print '\t'.join([obj['id'], lang, links[wiki]['title']])
