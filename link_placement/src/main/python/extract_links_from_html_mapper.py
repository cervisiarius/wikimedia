#!/usr/bin/python

import json, sys, errno, codecs, re, HTMLParser
from collections import defaultdict

# We want to read and write unicode.
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

HREF_PATTERN = '<a href="/wiki/'
TITLE_PATTERN = ' title="'
CONTENT_REGEX = re.compile(r'.*<div id="mw-content-text" lang="en" dir="ltr" class="mw-content-ltr">(.*)<div id="mw-navigation">')
TAG_REGEX = re.compile(r'<a href="/wiki/([^"]*)" title="([^"]*)(.*)">')
SPECIAL_REGEX = re.compile(r'(Wikipedia|Special|File|Template|Help|Talk|User|Book|Category):[^_]|.*_talk:[^_]')
parser = HTMLParser.HTMLParser()

def normalize(title):
  title = unicode(parser.unescape(title))
  title = title.replace(' ', '_')
  return title

if __name__ == '__main__':

  for line in sys.stdin:
    tokens = line.split('\t')
    if (len(tokens) >= 2):
      source = tokens[0]
      try:
        obj = json.loads(tokens[1])
        try:
          # This loop should only ever go through one iteration. To be sure, break after the first.
          for obj2 in obj['query']['pages'].values():
            pageid = obj2['pageid']
            title = normalize(obj2['title'])
            revid = obj2['revisions'][0]['revid']
            timestamp = obj2['revisions'][0]['timestamp']
            html = obj2['revisions'][0]['*']
            m = CONTENT_REGEX.match(html)
            if (m is not None):
              html = m.group(1)
              pos = defaultdict(list)
              size = 0
              i = 0
              while i < len(html):
                url_start = i + len(HREF_PATTERN)
                if html[i:url_start] == HREF_PATTERN:
                  tag_end = html.index('>', i) + 1
                  tag = html[i:tag_end]
                  m = TAG_REGEX.match(tag)
                  if m is not None:
                    target = normalize(m.group(2))
                    # Ignore links to special pages.
                    if (SPECIAL_REGEX.match(target) is None):
#                      pos[target].append(str(i))
                      pos[target].append(str(size))
                  i = tag_end
                else if html[i] == '<':
                  i = html.index('>', i) + 1
                else:
                  size += 1
                  i += 1
              for target in pos.keys():
                # Ignore self-links.
                if target != title:
                  #print '\t'.join([title, target, str(len(html)), ','.join(pos[target])])
                  print '\t'.join([title, target, size, ','.join(pos[target])])
            break
        except KeyError:
          pass
          # sys.stderr.write(u"JSON doesn't have all required fields: {}\n".format(source))
      except ValueError:
        pass
        # sys.stderr.write(u'Illegal JSON: {}\n'.format(tokens[1]))
